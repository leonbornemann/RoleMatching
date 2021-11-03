package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import com.typesafe.scalalogging.StrictLogging
import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.creation.CompatibilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge

import java.io.{File, PrintWriter}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContextExecutor, Future}

abstract class AbstractAsynchronousRoleTree[A](val toGeneralEdgeFunction:((RoleReference[A],RoleReference[A]) => SimpleCompatbilityGraphEdge),
                                               val resultDir:File,
                                               val processName:String,
                                               val prOption:Option[PrintWriter],
                                               val isAsynch:Boolean=true,
                                               val externalRecurseDepth:Int,
                                               val loggingActive:Boolean=false
                                  ) extends StrictLogging{

  var totalNumTopLevelNodes = 0
  var processedTopLvlNodes = 0

  def logProgress: Boolean = externalRecurseDepth==0 && totalNumTopLevelNodes > 1000 && (totalNumTopLevelNodes / processedTopLvlNodes) % (totalNumTopLevelNodes / 1000)==0

  def maybeLogProgress() = {
    if(loggingActive)
      logger.debug(s"Root Process ($processName) finished ${100 * processedTopLvlNodes / totalNumTopLevelNodes.toDouble}% of top-lvl nodes")
  }

  def loggingIsActive: Boolean = externalRecurseDepth==0 || loggingActive


  def thresholdForFork = AbstractAsynchronousRoleTree.thresholdForFork

  if(!isAsynch && !prOption.isDefined){
    logger.debug("That is weird - we are probably overwriting an existing file")
    assert(false)
  }

  var fnameOfWriter:Option[String] = None

  val pr = if(prOption.isDefined)
    prOption.get else {
      val (writer,fname) = ConcurrentCompatiblityGraphCreator.getOrCreateNewPrintWriter(resultDir)
      fnameOfWriter = Some(fname)
      writer
    }

  val mySubNodeFutures = scala.collection.mutable.HashMap[String,Future[Any]]()
  var parallelRecurseCounter = 0
  var internalRecurseCounter = 0
  init()

  def init() = {
    if(isAsynch)
      logger.debug(s"Created new Asynchronously running process $processName")
    execute()
    if(prOption.isEmpty)
      ConcurrentCompatiblityGraphCreator.releasePrintWriter(pr,fnameOfWriter.get)
      //pr.close()
  }

  def execute():Unit

  def getGraphConfig: GraphConfig

  def serializeIfMatch(tr1:RoleReference[A], tr2:RoleReference[A], pr:PrintWriter) = {
    AbstractAsynchronousRoleTree.serializeIfMatch(tr1,tr2,pr,toGeneralEdgeFunction)
  }

  def partitionToIntervals(inputList: IndexedSeq[RoleReference[A]], border: Int) = {
    if(inputList.size<border){
      IndexedSeq((0,inputList.size))
    } else {
      val indexBordersWithIndex = (0 until inputList.size by border)
        .zipWithIndex
      val intervals = indexBordersWithIndex.map{case (index,i) => {
        if(i!=indexBordersWithIndex.size-1)
          (index,indexBordersWithIndex(i+1)._1)
        else
          (index,inputList.size)
      }}
      intervals
    }
  }
}
object AbstractAsynchronousRoleTree {

  def getTupleMatchOption[A](ref1:RoleReference[A], ref2:RoleReference[A]) = {
    val left = ref1.getDataTuple.head
    val right = ref2.getDataTuple.head // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
    val evidence = left.getOverlapEvidenceCount(right)
    if (evidence == -1) {
      None
    } else {
      Some(CompatibilityGraphEdge(ref1,ref2, evidence))
    }
  }

  def serializeIfMatch[A](tr1:RoleReference[A], tr2:RoleReference[A], pr:PrintWriter, toGeneralEdgeFunction:((RoleReference[A],RoleReference[A]) => SimpleCompatbilityGraphEdge)) = {
    val option = getTupleMatchOption(tr1,tr2)
    if(option.isDefined){
      val e = option.get
      val edge = toGeneralEdgeFunction(e.tupleReferenceA,e.tupleReferenceB)
      edge.appendToWriter(pr,false,true)
    }
  }

  //end borders are exclusive
  def startProcessIntervalsFromSameList[A](tuplesInNodeAsIndexedSeq: IndexedSeq[RoleReference[A]],
                                           i1: (Int, Int),
                                           i2: (Int, Int),
                                           resultDir:File,
                                           context:ExecutionContextExecutor,
                                           processName:String,
                                           futures:java.util.concurrent.ConcurrentHashMap[String,Future[Any]],
                                           toGeneralEdgeFunction:((RoleReference[A],RoleReference[A]) => SimpleCompatbilityGraphEdge),
                                           tupleToNonWcTransitions:Option[Map[RoleReference[A], Set[ValueTransition[A]]]]
                                          ) = {
    val f = Future{
      val (pr,fname) = ConcurrentCompatiblityGraphCreator.getOrCreateNewPrintWriter(resultDir)
      val (firstBorderStart,firstBorderEnd) = i1
      val (secondBorderStart,secondBorderEnd) = i2
      var matchChecks = 0
      for(i <- firstBorderStart until firstBorderEnd){
        for(j <- secondBorderStart until secondBorderEnd){
          val ref1 = tuplesInNodeAsIndexedSeq(i)
          val ref2 = tuplesInNodeAsIndexedSeq(j)
          if(i<j && (!tupleToNonWcTransitions.isDefined || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t)))){
            serializeIfMatch(ref1,ref2,pr,toGeneralEdgeFunction)
          }
          matchChecks+=1
        }
      }
      serializeMatchChecks(matchChecks)
      ConcurrentCompatiblityGraphCreator.releasePrintWriter(pr,fname)
    }(context)
    ConcurrentCompatiblityGraphCreator.setupFuture(f,processName,futures,context)
  }

  def serializeMatchChecks[A](matchChecks: Int) = {
    if (matchChecks != 0) {
      val (prStats, fnameStats) = ConcurrentCompatiblityGraphCreator.getOrCreateNewStatsPrintWriter(GLOBAL_CONFIG.INDEXING_STATS_RESULT_DIR)
      prStats.println(matchChecks)
      ConcurrentCompatiblityGraphCreator.releaseStatPrintWriter(prStats, fnameStats)
    }
  }

  def startProcessIntervalsFromBipariteList[A](tuplesLeft: IndexedSeq[RoleReference[A]], tuplesRight: IndexedSeq[RoleReference[A]], i1: (Int, Int), i2: (Int, Int), resultDir: File, context: ExecutionContextExecutor, processName: String, futures: ConcurrentHashMap[String, Future[Any]], toGeneralEdgeFunction: (RoleReference[A], RoleReference[A]) => SimpleCompatbilityGraphEdge, tupleToNonWcTransitions: Option[Map[RoleReference[A], Set[ValueTransition[A]]]]) = {
    val f = Future{
      val (pr,fname) = ConcurrentCompatiblityGraphCreator.getOrCreateNewPrintWriter(resultDir)
      val (firstBorderStart,firstBorderEnd) = i1
      val (secondBorderStart,secondBorderEnd) = i2
      var matchChecks = 0
      for(i <- firstBorderStart until firstBorderEnd){
        for(j <- secondBorderStart until secondBorderEnd){
          val ref1 = tuplesLeft(i)
          val ref2 = tuplesRight(j)
          if(!tupleToNonWcTransitions.isDefined || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t))){
            serializeIfMatch(ref1,ref2,pr,toGeneralEdgeFunction)
          }
          matchChecks+=1
        }
      }
      serializeMatchChecks(matchChecks)
      ConcurrentCompatiblityGraphCreator.releasePrintWriter(pr,fname)
    }(context)
    ConcurrentCompatiblityGraphCreator.setupFuture(f,processName,futures,context)
  }

  var thresholdForFork = 2000
  var maxPairwiseListSizeForSingleThread = 30

}
