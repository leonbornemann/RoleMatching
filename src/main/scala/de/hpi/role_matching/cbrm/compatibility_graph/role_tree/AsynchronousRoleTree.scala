package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import com.typesafe.scalalogging.StrictLogging
import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.creation.AbstractAsynchronousRoleTree.maxPairwiseListSizeForSingleThread
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.concurrent.{ExecutionContextExecutor, Future}

class AsynchronousRoleTree[A](tuples: IndexedSeq[RoleReference[A]],
                              val parentNodesTimestamps:IndexedSeq[LocalDate],
                              val parentNodesKeys:IndexedSeq[A],
                              graphConfig:GraphConfig,
                              nonInformativeValues:Set[A] = Set[A](),
                              futures:java.util.concurrent.ConcurrentHashMap[String,Future[Any]],
                              context:ExecutionContextExecutor,
                              resultDir:File,
                              processName:String,
                              prOption:Option[PrintWriter],
                              toGeneralEdgeFunction:((RoleReference[A],RoleReference[A]) => SimpleCompatbilityGraphEdge),
                              tupleToNonWcTransitions:Option[Map[RoleReference[A], Set[ValueTransition[A]]]],
                              isAsynch:Boolean=true,
                              externalRecurseDepth:Int,
                              logProgress:Boolean=false
  ) extends AbstractAsynchronousRoleTree[A](toGeneralEdgeFunction,resultDir,processName,prOption,isAsynch,externalRecurseDepth,logProgress) {

  override def execute() = {
    val index = new RoleTreeLevel[A](tuples,parentNodesTimestamps,parentNodesKeys,tuples.head.table.wildcardValues.toSet,true)
    if(loggingIsActive) {
      totalNumTopLevelNodes = if(!index.indexBuildWasSuccessfull) 0 else  index.tupleGroupIterator(true).size
      logger.debug(s"Root Process ($processName) about to process $totalNumTopLevelNodes top-lvl nodes")
    }
    buildGraph(tuples,index)
  }

  def buildGraph(originalInput:IndexedSeq[RoleReference[A]], index: RoleTreeLevel[A]):Unit = {
    //if(externalRecurseDepth ==0)
    if(index.indexBuildWasSuccessfull){
      val nonWildcards = collection.mutable.ArrayBuffer[RoleReference[A]]()
      index.tupleGroupIterator(true).foreach{case g => {
        nonWildcards ++= g.nonWildcardRoles
        if(squareProductTooBig(g.nonWildcardRoles.size)){
          //further index this: new Index
          if(g.nonWildcardRoles.size>thresholdForFork){
            val newName = processName + s"_$parallelRecurseCounter.json"
            //do this asynchrounously:
            val f = AsynchronousRoleTree.createAsFuture(futures,
              g.nonWildcardRoles.toIndexedSeq,
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
            new AsynchronousRoleTree[A](g.nonWildcardRoles.toIndexedSeq,
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
          val tuplesInNodeAsIndexedSeq = g.nonWildcardRoles.toIndexedSeq
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
          val f = AsynchronousRoleTree.createAsFuture(futures,
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
          new AsynchronousRoleTree[A](wildcardBucket,
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
          val f = AsynchronousBipartiteRoleTree.createAsFuture(futures,
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
          val bipartiteCreator = new AsynchronousBipartiteRoleTree[A](wildcardBucket,
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

  private def doPairwiseMatching(tuplesInNodeAsIndexedSeq: IndexedSeq[RoleReference[A]]) = {
    if(tuplesInNodeAsIndexedSeq.size > maxPairwiseListSizeForSingleThread){
      val border = maxPairwiseListSizeForSingleThread
      val intervals = partitionToIntervals(tuplesInNodeAsIndexedSeq,border)
      for (i <- 0 until intervals.size) {
        for (j <- i until intervals.size) {
          val i1 = intervals(i)
          val i2 = intervals(j)
          AbstractAsynchronousRoleTree.startProcessIntervalsFromSameList(tuplesInNodeAsIndexedSeq,
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
      var  matchChecks = 0
      for (i <- 0 until tuplesInNodeAsIndexedSeq.size) {
        for (j <- i + 1 until tuplesInNodeAsIndexedSeq.size) {
          val ref1 = tuplesInNodeAsIndexedSeq(i)
          val ref2 = tuplesInNodeAsIndexedSeq(j)
          if(!tupleToNonWcTransitions.isDefined || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t))){
            //we have a candidate - add it to buffer!
            serializeIfMatch(ref1,ref2,pr)
          }
          matchChecks+=1
        }
      }
      AbstractAsynchronousRoleTree.serializeMatchChecks(matchChecks)
    }
  }

  def gaussSum(n: Int) = n*n+1/2

  def squareProductTooBig(n:Int): Boolean = {
    if(gaussSum(n) > 50) true else false
  }

  override def getGraphConfig: GraphConfig = graphConfig
}
object AsynchronousRoleTree extends StrictLogging {

  def createAsFuture[A](futures:java.util.concurrent.ConcurrentHashMap[String,Future[Any]],
                        tuples: IndexedSeq[RoleReference[A]],
                        parentNodesTimestamps:IndexedSeq[LocalDate],
                        parentNodesKeys:IndexedSeq[A],
                        graphConfig:GraphConfig,
                        nonInformativeValues:Set[A] = Set[A](),
                        context:ExecutionContextExecutor,
                        resultDir:File,
                        fname:String,
                        toGeneralEdgeFunction:((RoleReference[A],RoleReference[A]) => SimpleCompatbilityGraphEdge),
                        tupleToNonWcTransitions:Option[Map[RoleReference[A], Set[ValueTransition[A]]]],
                        newExternalRecurseDepth:Int,
                        logProgress:Boolean) = {
    val f = Future {
      new AsynchronousRoleTree[A](tuples,
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
    ConcurrentCompatiblityGraphCreator.setupFuture(f,fname,futures,context)
    f
  }

}
