package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.AbstractAsynchronousRoleTree.maxPairwiseListSizeForSingleThread
import de.hpi.role_matching.cbrm.data.{RoleReference, ValueTransition}

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.concurrent.{ExecutionContextExecutor, Future}

class AsynchronousRoleTree(tuples: IndexedSeq[RoleReference],
                              val parentNodesTimestamps:IndexedSeq[LocalDate],
                              val parentNodesKeys:IndexedSeq[Any],
                              graphConfig:GraphConfig,
                              nonInformativeValues:Set[Any] = Set[Any](),
                              futures:java.util.concurrent.ConcurrentHashMap[String,Future[Any]],
                              context:ExecutionContextExecutor,
                              resultDir:File,
                              processName:String,
                              prOption:Option[PrintWriter],
                              toGeneralEdgeFunction:((RoleReference,RoleReference) => SimpleCompatbilityGraphEdge),
                              tupleToNonWcTransitions:Option[Map[RoleReference, Set[ValueTransition]]],
                              isAsynch:Boolean=true,
                              externalRecurseDepth:Int,
                              logProgress:Boolean=false,
                              serializeGroupsOnly:Boolean
  ) extends AbstractAsynchronousRoleTree(toGeneralEdgeFunction,resultDir,processName,prOption,isAsynch,externalRecurseDepth,logProgress,serializeGroupsOnly) {

  override def execute() = {
    val index = new RoleTreeLevel(tuples,parentNodesTimestamps,parentNodesKeys,tuples.head.roles.wildcardValues.toSet,true)
    if(loggingIsActive) {
      totalNumTopLevelNodes = if(!index.indexBuildWasSuccessfull) 0 else  index.tupleGroupIterator(true).size
      logger.debug(s"Root Process ($processName) about to process $totalNumTopLevelNodes top-lvl nodes")
    }
    buildGraph(tuples,index)
  }

  def buildGraph(originalInput:IndexedSeq[RoleReference], index: RoleTreeLevel):Unit = {
    //if(externalRecurseDepth ==0)
    if(index.indexBuildWasSuccessfull){
      val nonWildcards = collection.mutable.ArrayBuffer[RoleReference]()
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
              false,
              serializeGroupsOnly
            )
            parallelRecurseCounter+=1
            mySubNodeFutures.put(newName,f)
          } else {
            //do it in this thread:
            new AsynchronousRoleTree(g.nonWildcardRoles.toIndexedSeq,
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
              externalRecurseDepth+1,
              false,
              serializeGroupsOnly)
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
            externalRecurseDepth==0,
            serializeGroupsOnly)
          parallelRecurseCounter += 1
          mySubNodeFutures.put(newName,f)
        } else {
          new AsynchronousRoleTree(wildcardBucket,
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
            externalRecurseDepth==0,
            logProgress)
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
            index.parentNodesKeys ++ Seq(wildcardBucket.head.roles.wildcardValues.head),
            graphConfig,
            nonInformativeValues,
            context,
            resultDir,
            newName,
            toGeneralEdgeFunction,
            tupleToNonWcTransitions,
            externalRecurseDepth,
            serializeGroupsOnly)
          parallelRecurseCounter+=1
          mySubNodeFutures.put(newName,f)
        } else {
          //do it in this thread
          val bipartiteCreator = new AsynchronousBipartiteRoleTree(wildcardBucket,
            nonWildcards.toIndexedSeq,
            index.indexedTimestamps.toIndexedSeq,
            index.parentNodesKeys ++ Seq(wildcardBucket.head.roles.wildcardValues.head),
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
            externalRecurseDepth,
            false,
            serializeGroupsOnly
          )
        }
      }
    } else {
      doPairwiseMatching(originalInput)
    }
  }

  private def doPairwiseMatching(tuplesInNodeAsIndexedSeq: IndexedSeq[RoleReference]) = {
    if(serializeGroupsOnly){
      serializeGroup(tuplesInNodeAsIndexedSeq)
    } else {
      if(tuplesInNodeAsIndexedSeq.size > AbstractAsynchronousRoleTree.maxPairwiseListSizeForSingleThread){
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
            val evidence = ref1.nonWildCardChangePointsInTrainPeriod.intersect(ref2.nonWildCardChangePointsInTrainPeriod).size
            if(evidence>1){
              //            val e = toGeneralEdgeFunction(ref1,ref2)
              //            if(e.v1.id == "infobox book||1000149||161384507-0||country" && e.v2.id == "infobox book||2730873||259414743-0||image" || e.v2.id == "infobox book||1000149||161384507-0||country" && e.v1.id == "infobox book||2730873||259414743-0||image")
              //              println()
              serializeIfMatch(ref1,ref2,pr)
            }
            //          if(!tupleToNonWcTransitions.isDefined || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t))){
            //            //we have a candidate - add it to buffer!
            //            serializeIfMatch(ref1,ref2,pr)
            //          }
            matchChecks+=1
          }
        }
        AbstractAsynchronousRoleTree.serializeMatchChecks(matchChecks)
      }
    }
  }

  private def fulfillsFIlter(ref1: RoleReference, ref2: RoleReference) = {
    ref1.getRole.lineage.values.toIndexedSeq.exists(s => s.toString.contains("[[File:The Machine Gunners cover.jpg|200px]]")) &&
      ref2.getRole.lineage.values.toIndexedSeq.exists(s => s.toString.contains("[[United States]]"))
  }

  def gaussSum(n: Int) = n*n+1/2

  def squareProductTooBig(n:Int): Boolean = {
    if(gaussSum(n) > 50) true else false
  }

  override def getGraphConfig: GraphConfig = graphConfig
}
object AsynchronousRoleTree extends StrictLogging {

  def createAsFuture(futures:java.util.concurrent.ConcurrentHashMap[String,Future[Any]],
                        tuples: IndexedSeq[RoleReference],
                        parentNodesTimestamps:IndexedSeq[LocalDate],
                        parentNodesKeys:IndexedSeq[Any],
                        graphConfig:GraphConfig,
                        nonInformativeValues:Set[Any] = Set[Any](),
                        context:ExecutionContextExecutor,
                        resultDir:File,
                        fname:String,
                        toGeneralEdgeFunction:((RoleReference,RoleReference) => SimpleCompatbilityGraphEdge),
                        tupleToNonWcTransitions:Option[Map[RoleReference, Set[ValueTransition]]],
                        newExternalRecurseDepth:Int,
                        logProgress:Boolean,
                        serializeGroupsOnly:Boolean) = {
    val f = Future {
      new AsynchronousRoleTree(tuples,
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
        logProgress,
        serializeGroupsOnly)
    }(context)
    ConcurrentCompatiblityGraphCreator.setupFuture(f,fname,futures,context)
    f
  }

}
