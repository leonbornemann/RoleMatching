package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.AbstractAsynchronousRoleTree.maxPairwiseListSizeForSingleThread
import de.hpi.role_matching.cbrm.data.{RoleReference, ValueTransition}

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContextExecutor, Future}


class AsynchronousBipartiteRoleTree(tuplesLeft: IndexedSeq[RoleReference],
                                       tuplesRight: IndexedSeq[RoleReference],
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
                                       logProgress:Boolean=false
                                  ) extends AbstractAsynchronousRoleTree(toGeneralEdgeFunction,resultDir, processName,prOption, isAsynch,externalRecurseDepth,logProgress) {

  override def execute() = {
    if(externalRecurseDepth==0){
      println()
    }
    val index = new BipartiteRoleTreeLevel(tuplesLeft,tuplesRight,parentNodesTimestamps,parentNodesKeys,true)
    if(loggingIsActive) {
      totalNumTopLevelNodes = if(index.indexFailed) 0 else  index.getBipartiteTupleGroupIterator().size
      logger.debug(s"Bipartite Root ($processName) indexFailed status: ${index.indexFailed}")
      logger.debug(s"Bipartite Root ($processName) Process about to process $totalNumTopLevelNodes top-lvl nodes")
    }
    buildGraph(tuplesLeft,tuplesRight,index)
  }

  def productTooBig(size: Int, size1: Int): Boolean = {
    size*size1>50
  }

  def buildGraph(originalInputLeft:IndexedSeq[RoleReference],
                 originalInputRight:IndexedSeq[RoleReference],
                 index: BipartiteRoleTreeLevel):Unit = {
    if(!index.indexFailed){
      val allTuplesLeft = scala.collection.mutable.ArrayBuffer[RoleReference]()
      val allTuplesRight = scala.collection.mutable.ArrayBuffer[RoleReference]()
      index.getBipartiteTupleGroupIterator().foreach{case g => {
        val tuplesLeft = g.tuplesLeft
        val tuplesRight = g.tuplesRight
        buildGraphRecursively(g.chosenTimestamps.toIndexedSeq,g.valuesAtTimestamps, tuplesLeft, tuplesRight)
        //TODO: process Wildcards to others:
        allTuplesLeft ++= tuplesLeft
        allTuplesRight ++= tuplesRight
        this.processedTopLvlNodes += 1
        maybeLogProgress()
      }}
      buildGraphRecursively(index.parentTimestamps ++Seq(index.splitT),index.parentKeyValues ++Seq(index.wildcardValues.head),index.wildcardsLeft,index.wildcardsRight)
      if(loggingIsActive)
        logger.debug(s"Bipartite Root Process ($processName) is done with WC Left to WC Right")
      buildGraphRecursively(index.parentTimestamps ++Seq(index.splitT),index.parentKeyValues ++Seq(index.wildcardValues.head),index.wildcardsLeft,allTuplesRight.toIndexedSeq)
      if(loggingIsActive)
        logger.debug(s"Bipartite Root Process ($processName) is done with WC Left to Tuples Right")
      buildGraphRecursively(index.parentTimestamps ++Seq(index.splitT),index.parentKeyValues ++Seq(index.wildcardValues.head),allTuplesLeft.toIndexedSeq,index.wildcardsRight)
      if(loggingIsActive)
        logger.debug(s"Bipartite Root Process ($processName) is done with Tuples Left to WC Right")
    } else {
      doPairwiseMatching(originalInputLeft,originalInputRight)
    }
  }

  private def buildGraphRecursively(parentTimestamps:IndexedSeq[LocalDate],
                                    parentValues:IndexedSeq[Any],
                                    tuplesLeft: IndexedSeq[RoleReference],
                                    tuplesRight: IndexedSeq[RoleReference]) = {
    if (productTooBig(tuplesLeft.size, tuplesRight.size)) {
      //further index this: new Index
      if(tuplesLeft.size + tuplesRight.size > thresholdForFork){
        val newName = processName + s"_$parallelRecurseCounter"
        val f = AsynchronousBipartiteRoleTree.createAsFuture(futures,
          tuplesLeft,
          tuplesRight,
          parentTimestamps,
          parentValues,
          graphConfig,
          nonInformativeValues,
          context,
          resultDir,
          newName,
          toGeneralEdgeFunction,
          tupleToNonWcTransitions,
          externalRecurseDepth+1)
        parallelRecurseCounter += 1
        mySubNodeFutures.put(newName,f)
      } else {
        new AsynchronousBipartiteRoleTree(
          tuplesLeft,
          tuplesRight,
          parentTimestamps,
          parentValues,
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
    } else {
      doPairwiseMatching(tuplesLeft, tuplesRight)
    }
  }

  private def doPairwiseMatching(tuplesLeft: IndexedSeq[RoleReference], tuplesRight:IndexedSeq[RoleReference]) = {
    //we construct a graph as an adjacency list:
    //pairwise matching to find out the edge-weights:
    if(tuplesLeft.size*tuplesRight.size > maxPairwiseListSizeForSingleThread*maxPairwiseListSizeForSingleThread){
      val maxSize = maxPairwiseListSizeForSingleThread
      val intervals1 = partitionToIntervals(tuplesLeft,maxSize)
      val intervals2 = partitionToIntervals(tuplesRight,maxSize)
      for (i <- 0 until intervals1.size) {
        for (j <- 0 until intervals2.size) {
          val i1 = intervals1(i)
          val i2 = intervals2(j)
          AbstractAsynchronousRoleTree.startProcessIntervalsFromBipariteList(tuplesLeft,
            tuplesRight,
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
      if (tuplesLeft.size > 0 && tuplesRight.size > 0) {
        var matchChecks=0
        for (i <- 0 until tuplesLeft.size) {
          for (j <- 0 until tuplesRight.size) {
            val ref1 = tuplesLeft(i)
            val ref2 = tuplesRight(j)
            if (!tupleToNonWcTransitions.isDefined || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t))) {
              serializeIfMatch(ref1, ref2, pr)
            }
            matchChecks+=1
          }
        }
        AbstractAsynchronousRoleTree.serializeMatchChecks(matchChecks)
      }
    }
  }

  override def getGraphConfig: GraphConfig = graphConfig
}
object AsynchronousBipartiteRoleTree extends StrictLogging {
  def createAsFuture(futures: ConcurrentHashMap[String,Future[Any]],
                        tuplesLeft: IndexedSeq[RoleReference],
                        tuplesRight: IndexedSeq[RoleReference],
                        parentTimestamps: IndexedSeq[LocalDate],
                        parentValues: IndexedSeq[Any],
                        graphConfig: GraphConfig,
                        nonInformativeValues: Set[Any],
                        context: ExecutionContextExecutor,
                        resultDir: File,
                        fname: String,
                        toGeneralEdgeFunction: (RoleReference, RoleReference) => SimpleCompatbilityGraphEdge,
                        tupleToNonWcTransitions: Option[Map[RoleReference, Set[ValueTransition]]],
                        externalRecurseDepth:Int) = {
    val f = Future {
      new AsynchronousBipartiteRoleTree(
        tuplesLeft,
        tuplesRight,
        parentTimestamps,
        parentValues,
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
        externalRecurseDepth)
    }(context)
    ConcurrentCompatiblityGraphCreator.setupFuture(f,fname,futures,context)
    f
  }

}
