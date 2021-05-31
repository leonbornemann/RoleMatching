package de.hpi.tfm.compatibility.graph.fact.internal

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.bipartite.BipartiteFactMatchCreator
import de.hpi.tfm.compatibility.graph.fact.{FactMatchCreator, ParallelBatchExecutor, TupleReference}
import de.hpi.tfm.compatibility.index.TupleSetIndex
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.tfm.evaluation.data.GeneralEdge

import java.io.File
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

class InternalFactMatchGraphCreator[A](tuples: IndexedSeq[TupleReference[A]],
                                       graphConfig:GraphConfig,
                                       filterByCommonWildcardIgnoreChangeTransition:Boolean=true,
                                       nonInformativeValues:Set[A] = Set[A](),
                                       futures:java.util.concurrent.ConcurrentHashMap[Future[String], Boolean],
                                       context:ExecutionContextExecutor,
                                       resultDir:File,
                                       toGeneralEdgeFunction:((TupleReference[A],TupleReference[A]) => GeneralEdge),
                                       service:Option[ExecutorService] // if specified, this will be shut down after everything has terminated
  ) extends FactMatchCreator[A](new ParallelBatchExecutor[A](futures,context,resultDir,toGeneralEdgeFunction)) {

  var tupleToNonWcTransitions:Option[Map[TupleReference[A], Set[ValueTransition[A]]]] = None

  if(filterByCommonWildcardIgnoreChangeTransition){
    tupleToNonWcTransitions = Some(tuples
      .map(t => (t,t.getDataTuple.head
        .valueTransitions(false,true)
        .filter(t => !nonInformativeValues.contains(t.prev) && !nonInformativeValues.contains(t.after))
      ))
      .toMap)
  }
  init()
  var totalNumTopLevelNodes = -1
  var processedTopLvlNodes = 0

  def init() = {
    val index = new TupleSetIndex[A](tuples,IndexedSeq(),IndexedSeq(),tuples.head.table.wildcardValues.toSet,true)
    totalNumTopLevelNodes = if(!index.indexBuildWasSuccessfull) 0 else  index.tupleGroupIterator(true).size
    logger.debug(s"Iterating through $totalNumTopLevelNodes")
    buildGraph(tuples,index,0)
    logger.debug("Executing remaining elements")
    parallelBatchExecutor.startCurrentBatchIfNotEmpty()
    if(service.isDefined){
      while(!futures.isEmpty){
        Thread.sleep(10000)
        logger.debug(s"There are currently ${futures.size()} batches in the queue")
      }
      logger.debug("Completed - executor is now shut down")
      service.get.shutdownNow()
    }
  }

  def buildGraph(originalInput:IndexedSeq[TupleReference[A]], index: TupleSetIndex[A],recurseDepth:Int):Unit = {
    if(index.indexBuildWasSuccessfull){
      val nonWildcards = collection.mutable.ArrayBuffer[TupleReference[A]]()
      index.tupleGroupIterator(true).foreach{case g => {
        nonWildcards ++= g.tuplesInNode
        if(squareProductTooBig(g.tuplesInNode.size)){
          //further index this: new Index
          val newIndexForSubNode = new TupleSetIndex[A](g.tuplesInNode.toIndexedSeq,index.indexedTimestamps.toIndexedSeq,g.valuesAtTimestamps,index.wildcardKeyValues,true)
          buildGraph(g.tuplesInNode.toIndexedSeq,newIndexForSubNode,recurseDepth+1)
        } else{
          val tuplesInNodeAsIndexedSeq = g.tuplesInNode.toIndexedSeq
          doPairwiseMatching(tuplesInNodeAsIndexedSeq)
        }
        if(recurseDepth==0){
          processedTopLvlNodes+=1
          if(logProgress)
            logger.debug(s"Finished $processedTopLvlNodes out of $totalNumTopLevelNodes top level nodes(${100*processedTopLvlNodes / totalNumTopLevelNodes.toDouble}%) ")
        }
      }}
      //wildcards internally:
      val wildcardBucket = index.getWildcardBucket
      if(recurseDepth==0)
        logger.debug("Processing Wildcards internally")
      if(squareProductTooBig(wildcardBucket.size)){
        val newIndex = new TupleSetIndex[A](wildcardBucket,index.indexedTimestamps.toIndexedSeq,index.parentNodesKeys ++ Seq(index.wildcardKeyValues.head),index.wildcardKeyValues,true)
        buildGraph(wildcardBucket,newIndex,recurseDepth+1)
      } else {
        doPairwiseMatching(wildcardBucket)
      }
      if(recurseDepth==0)
        logger.debug("Finished Processing Wildcards internally")
      //wildcards to the rest:
      if(recurseDepth==0)
        logger.debug("Beginning to process wildcards to other nodes")
      if(wildcardBucket.size>0 && nonWildcards.size>0){
        val bipartiteCreator = new BipartiteFactMatchCreator[A](wildcardBucket,
          nonWildcards.toIndexedSeq,
          graphConfig,
          filterByCommonWildcardIgnoreChangeTransition,
          tupleToNonWcTransitions,
          nonInformativeValues,
          recurseDepth==0,
          false,
          parallelBatchExecutor
        )
      }
    } else {
      doPairwiseMatching(originalInput)
    }
  }

  def logProgress = processedTopLvlNodes % (totalNumTopLevelNodes/1000) == 0

  private def doPairwiseMatching(tuplesInNodeAsIndexedSeq: IndexedSeq[TupleReference[A]]) = {
    //we construct a graph as an adjacency list:
    //pairwise matching to find out the edge-weights:
    for (i <- 0 until tuplesInNodeAsIndexedSeq.size) {
      for (j <- i + 1 until tuplesInNodeAsIndexedSeq.size) {
        val ref1 = tuplesInNodeAsIndexedSeq(i)
        val ref2 = tuplesInNodeAsIndexedSeq(j)
        if(!filterByCommonWildcardIgnoreChangeTransition || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t))){
          //we have a candidate - add it to buffer!
          parallelBatchExecutor.addCandidateAndMaybeCreateBatch(ref1,ref2)
        }
      }
    }
  }

  def gaussSum(n: Int) = n*n+1/2

  def squareProductTooBig(n:Int): Boolean = {
    if(gaussSum(n) > 50) true else false
  }

  override def getGraphConfig: GraphConfig = graphConfig
}
object InternalFactMatchGraphCreator{



}
