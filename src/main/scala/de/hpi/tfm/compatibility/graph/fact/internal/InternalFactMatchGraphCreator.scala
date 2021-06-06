package de.hpi.tfm.compatibility.graph.fact.internal

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.FactMatchCreator.maxPairwiseListSizeForSingleThread
import de.hpi.tfm.compatibility.graph.fact.bipartite.BipartiteFactMatchCreator
import de.hpi.tfm.compatibility.graph.fact.bipartite.BipartiteFactMatchCreator.logger
import de.hpi.tfm.compatibility.graph.fact.{ConcurrentMatchGraphCreator, FactMatchCreator, TupleReference}
import de.hpi.tfm.compatibility.index.TupleSetIndex
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.tfm.evaluation.data.GeneralEdge

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class InternalFactMatchGraphCreator[A](tuples: IndexedSeq[TupleReference[A]],
                                       val parentNodesTimestamps:IndexedSeq[LocalDate],
                                       val parentNodesKeys:IndexedSeq[A],
                                       graphConfig:GraphConfig,
                                       nonInformativeValues:Set[A] = Set[A](),
                                       futures:java.util.concurrent.ConcurrentHashMap[String,Future[Any]],
                                       context:ExecutionContextExecutor,
                                       resultDir:File,
                                       processName:String,
                                       prOption:Option[PrintWriter],
                                       toGeneralEdgeFunction:((TupleReference[A],TupleReference[A]) => GeneralEdge),
                                       tupleToNonWcTransitions:Option[Map[TupleReference[A], Set[ValueTransition[A]]]],
                                       isAsynch:Boolean=true,
                                       externalRecurseDepth:Int,
                                       logProgress:Boolean=false
  ) extends FactMatchCreator[A](toGeneralEdgeFunction,resultDir,processName,prOption,isAsynch,externalRecurseDepth,logProgress) {

  override def execute() = {
    val index = new TupleSetIndex[A](tuples,parentNodesTimestamps,parentNodesKeys,tuples.head.table.wildcardValues.toSet,true)
    if(loggingIsActive) {
      totalNumTopLevelNodes = if(!index.indexBuildWasSuccessfull) 0 else  index.tupleGroupIterator(true).size
      logger.debug(s"Root Process ($processName) about to process $totalNumTopLevelNodes top-lvl nodes")
    }
    buildGraph(tuples,index)
  }

  def buildGraph(originalInput:IndexedSeq[TupleReference[A]], index: TupleSetIndex[A]):Unit = {
    //if(externalRecurseDepth ==0)
    if(index.indexBuildWasSuccessfull){
      val nonWildcards = collection.mutable.ArrayBuffer[TupleReference[A]]()
      index.tupleGroupIterator(true).foreach{case g => {
        nonWildcards ++= g.tuplesInNode
        if(squareProductTooBig(g.tuplesInNode.size)){
          //further index this: new Index
          if(g.tuplesInNode.size>thresholdForFork){
            val newName = processName + s"_$parallelRecurseCounter.json"
            //do this asynchrounously:
            val f = InternalFactMatchGraphCreator.createAsFuture(futures,
              g.tuplesInNode.toIndexedSeq,
              index.indexedTimestamps.toIndexedSeq,
              g.valuesAtTimestamps,
              graphConfig,
              nonInformativeValues,
              context,
              resultDir,
              newName,
              toGeneralEdgeFunction,
              tupleToNonWcTransitions,
              externalRecurseDepth+1,
              false
            )
            parallelRecurseCounter+=1
            mySubNodeFutures.put(newName,f)
          } else {
            //do it in this thread:
            new InternalFactMatchGraphCreator[A](g.tuplesInNode.toIndexedSeq,
              index.indexedTimestamps.toIndexedSeq,
              g.valuesAtTimestamps,
              graphConfig,
              nonInformativeValues,
              futures,
              context,
              resultDir,
              processName + s"_rI_$internalRecurseCounter",
              Some(pr),
              toGeneralEdgeFunction,
              tupleToNonWcTransitions,
              false,
              externalRecurseDepth+1)
            internalRecurseCounter+=1
          }
        } else{
          val tuplesInNodeAsIndexedSeq = g.tuplesInNode.toIndexedSeq
          doPairwiseMatching(tuplesInNodeAsIndexedSeq)
        }
        processedTopLvlNodes +=1
        maybeLogProgress()
      }}
      //wildcards internally:
      if(loggingIsActive)
        logger.debug(s"Root Process ($processName) starting wildcard node (internally)")
      val wildcardBucket = index.getWildcardBucket
      if(squareProductTooBig(wildcardBucket.size)){
        val newName = processName + s"_WC$parallelRecurseCounter"
        if(wildcardBucket.size>thresholdForFork){
          val f = InternalFactMatchGraphCreator.createAsFuture(futures,
            wildcardBucket,
            index.indexedTimestamps.toIndexedSeq,
            index.parentNodesKeys ++ Seq(index.wildcardKeyValues.head),
            graphConfig,
            nonInformativeValues,
            context,
            resultDir,
            newName,
            toGeneralEdgeFunction,
            tupleToNonWcTransitions,
            externalRecurseDepth+1,
            externalRecurseDepth==0)
          parallelRecurseCounter += 1
          mySubNodeFutures.put(newName,f)
        } else {
          new InternalFactMatchGraphCreator[A](wildcardBucket,
            index.indexedTimestamps.toIndexedSeq,
            index.parentNodesKeys ++ Seq(index.wildcardKeyValues.head),
            graphConfig,
            nonInformativeValues,
            futures,
            context,
            resultDir,
            newName,
            Some(pr),
            toGeneralEdgeFunction,
            tupleToNonWcTransitions,
            false,
            externalRecurseDepth+1,
            externalRecurseDepth==0)
        }
      } else {
        doPairwiseMatching(wildcardBucket)
      }
      //wildcards to the rest:
      if(loggingIsActive)
        logger.debug(s"Root Process ($processName) starting wildcards to the rest")
      if(wildcardBucket.size>0 && nonWildcards.size>0){
        val newName = processName + s"_bipartite"
        if(externalRecurseDepth==0){
          println()
        }
        if(wildcardBucket.size + nonWildcards.size > thresholdForFork){
          //create future:
          if(externalRecurseDepth==0){
            println()
          }
          val f = BipartiteFactMatchCreator.createAsFuture(futures,
            wildcardBucket,
            nonWildcards.toIndexedSeq,
            index.indexedTimestamps.toIndexedSeq,
            index.parentNodesKeys ++ Seq(wildcardBucket.head.table.wildcardValues.head),
            graphConfig,
            nonInformativeValues,
            context,
            resultDir,
            newName,
            toGeneralEdgeFunction,
            tupleToNonWcTransitions,
            externalRecurseDepth)
          parallelRecurseCounter+=1
          mySubNodeFutures.put(newName,f)
        } else {
          //do it in this thread
          val bipartiteCreator = new BipartiteFactMatchCreator[A](wildcardBucket,
            nonWildcards.toIndexedSeq,
            index.indexedTimestamps.toIndexedSeq,
            index.parentNodesKeys ++ Seq(wildcardBucket.head.table.wildcardValues.head),
            graphConfig,
            nonInformativeValues,
            futures,
            context,
            resultDir,
            newName,
            Some(pr),
            toGeneralEdgeFunction,
            tupleToNonWcTransitions,
            false,
            externalRecurseDepth
          )
        }
      }
    } else {
      doPairwiseMatching(originalInput)
    }
  }

  private def doPairwiseMatching(tuplesInNodeAsIndexedSeq: IndexedSeq[TupleReference[A]]) = {
    if(tuplesInNodeAsIndexedSeq.size > maxPairwiseListSizeForSingleThread){
      val border = maxPairwiseListSizeForSingleThread
      val intervals = partitionToIntervals(tuplesInNodeAsIndexedSeq,border)
      for (i <- 0 until intervals.size) {
        for (j <- i until intervals.size) {
          val i1 = intervals(i)
          val i2 = intervals(j)
          FactMatchCreator.startProcessIntervalsFromSameList(tuplesInNodeAsIndexedSeq,
            i1,
            i2,
            resultDir,
            context,
            processName + s"PWM($i1,$i2)",
            futures,
            toGeneralEdgeFunction,
            tupleToNonWcTransitions)
        }
      }
    } else {
      //do it in this process!
      //we construct a graph as an adjacency list:
      //pairwise matching to find out the edge-weights:
      for (i <- 0 until tuplesInNodeAsIndexedSeq.size) {
        for (j <- i + 1 until tuplesInNodeAsIndexedSeq.size) {
          val ref1 = tuplesInNodeAsIndexedSeq(i)
          val ref2 = tuplesInNodeAsIndexedSeq(j)
          if(!tupleToNonWcTransitions.isDefined || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t))){
            //we have a candidate - add it to buffer!
            serializeIfMatch(ref1,ref2,pr)
          }
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
object InternalFactMatchGraphCreator extends StrictLogging {

  def createAsFuture[A](futures:java.util.concurrent.ConcurrentHashMap[String,Future[Any]],
                        tuples: IndexedSeq[TupleReference[A]],
                        parentNodesTimestamps:IndexedSeq[LocalDate],
                        parentNodesKeys:IndexedSeq[A],
                        graphConfig:GraphConfig,
                        nonInformativeValues:Set[A] = Set[A](),
                        context:ExecutionContextExecutor,
                        resultDir:File,
                        fname:String,
                        toGeneralEdgeFunction:((TupleReference[A],TupleReference[A]) => GeneralEdge),
                        tupleToNonWcTransitions:Option[Map[TupleReference[A], Set[ValueTransition[A]]]],
                        newExternalRecurseDepth:Int,
                        logProgress:Boolean) = {
    val f = Future {
      new InternalFactMatchGraphCreator[A](tuples,
        parentNodesTimestamps,
        parentNodesKeys,
        graphConfig,
        nonInformativeValues,
        futures,
        context,
        resultDir,
        fname,
        None,
        toGeneralEdgeFunction,
        tupleToNonWcTransitions,
        true,
        newExternalRecurseDepth,
        logProgress)
    }(context)
    ConcurrentMatchGraphCreator.setupFuture(f,fname,futures,context)
    f
  }

}
