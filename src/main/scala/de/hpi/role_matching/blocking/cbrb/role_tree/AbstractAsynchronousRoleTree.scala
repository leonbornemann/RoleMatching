package de.hpi.role_matching.blocking.cbrb.role_tree

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.blocking.cbrb.CBRBConfig
import de.hpi.role_matching.blocking.cbrb.role_tree.bipartite.{BipartitePairwiseTaskList, BipartiteRoleGroup}
import de.hpi.role_matching.blocking.cbrb.role_tree.normal.{NormalPairwiseTaskList, NormalRoleGroup}
import de.hpi.role_matching.data.{RoleMatchCandidate, RoleMatchCandidateIds, RoleReference, ValueTransition}
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContextExecutor, Future}

abstract class AbstractAsynchronousRoleTree(val toGeneralEdgeFunction:((RoleReference,RoleReference) => RoleMatchCandidate),
                                            val resultDir:File,
                                            val processName:String,
                                            val prOption:Option[PrintWriter],
                                            val isAsynch:Boolean=true,
                                            val externalRecurseDepth:Int,
                                            val loggingActive:Boolean=false,
                                            val serializeGroupsOnly:Boolean
                                  ) extends StrictLogging{

  var totalNumTopLevelNodes = 0
  var processedTopLvlNodes = 0

  def logProgress: Boolean = externalRecurseDepth==0 && totalNumTopLevelNodes > 1000 && (totalNumTopLevelNodes / processedTopLvlNodes) % (totalNumTopLevelNodes / 1000)==0

  def maybeLogProgress() = {
    if(loggingIsActive)
      logger.debug(s"Root Process ($processName) finished ${100 * processedTopLvlNodes / totalNumTopLevelNodes.toDouble}% of top-lvl nodes")
  }

  def loggingIsActive: Boolean = false //externalRecurseDepth==0 || loggingActive


  def thresholdForFork = AbstractAsynchronousRoleTree.thresholdForFork

  if(!isAsynch && !prOption.isDefined){
    logger.debug("That is weird - we are probably overwriting an existing file")
    assert(false)
  }

  var fnameOfWriter:Option[String] = None

  val pr = if(prOption.isDefined)
    prOption.get else {
      val (writer,fname) = ConcurrentRoleTreeTraverser.getOrCreateNewPrintWriter(resultDir)
      fnameOfWriter = Some(fname)
      writer
    }

  val mySubNodeFutures = scala.collection.mutable.HashMap[String,Future[Any]]()
  var parallelRecurseCounter = 0
  var internalRecurseCounter = 0
  init()

  def finishLastTaskList()

  def init() = {
    if(isAsynch && loggingIsActive)
      logger.debug(s"Created new Asynchronously running process $processName")
    execute()
    finishLastTaskList()
    if(prOption.isEmpty)
      ConcurrentRoleTreeTraverser.releasePrintWriter(pr,fnameOfWriter.get)
      //pr.close()
  }

  def execute():Unit

  def getGraphConfig: CBRBConfig

  def serializeIfMatch(tr1:RoleReference, tr2:RoleReference, pr:PrintWriter) = {
    AbstractAsynchronousRoleTree.serializeIfMatch(tr1,tr2,pr,toGeneralEdgeFunction)
  }

  def serializeGroup(trs:IndexedSeq[RoleReference]) = {
    NormalRoleGroup(trs.map(tr => tr.getRoleID)).appendToWriter(pr,false,true)
  }

  def serializeBipartiteGroup(left:IndexedSeq[RoleReference],right:IndexedSeq[RoleReference]) = {
    BipartiteRoleGroup(left.map(tr => tr.getRoleID),right.map(tr => tr.getRoleID)).appendToWriter(pr,false,true)
  }

  def partitionToIntervals(inputList: IndexedSeq[RoleReference], border: Int) = {
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

  def getTupleMatchOption(ref1:RoleReference, ref2:RoleReference) = {
    val left = ref1.getRole
    val right = ref2.getRole // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
    val evidence = left.getOverlapEvidenceCount(right)
    if (evidence == -1) {
      None
    } else {
      Some(FullyCompatibleCandidate(ref1,ref2, evidence))
    }
  }

  def serializeIfMatch(tr1:RoleReference, tr2:RoleReference, pr:PrintWriter, toGeneralEdgeFunction:((RoleReference,RoleReference) => RoleMatchCandidate)) = {
    val option = getTupleMatchOption(tr1,tr2)
    if(option.isDefined){
      val e = option.get
      val edge = toGeneralEdgeFunction(e.tupleReferenceA,e.tupleReferenceB)
      val idEdge = RoleMatchCandidateIds(edge.v1.id,edge.v2.id)
      idEdge.appendToWriter(pr,false,true)
    }
  }

  //end borders are exclusive
  def startProcessIntervalsFromSameList(tasks:NormalPairwiseTaskList,
                                        resultDir:File,
                                        context:ExecutionContextExecutor,
                                        processName:String,
                                        futures:java.util.concurrent.ConcurrentHashMap[String,Future[Any]],
                                        toGeneralEdgeFunction:((RoleReference,RoleReference) => RoleMatchCandidate),
                                        tupleToNonWcTransitions:Option[Map[RoleReference, Set[ValueTransition]]]
                                          ) = {
    val f = Future{
      val (pr,fname) = ConcurrentRoleTreeTraverser.getOrCreateNewPrintWriter(resultDir)
      doPairwiseMatchingSingleList(tasks, toGeneralEdgeFunction, tupleToNonWcTransitions, pr)
      ConcurrentRoleTreeTraverser.releasePrintWriter(pr,fname)
    }(context)
    ConcurrentRoleTreeTraverser.setupFuture(f,processName,futures,context)
  }

  def doPairwiseMatchingSingleList(tasks: NormalPairwiseTaskList, toGeneralEdgeFunction: (RoleReference, RoleReference) => RoleMatchCandidate, tupleToNonWcTransitions: Option[Map[RoleReference, Set[ValueTransition]]], pr: PrintWriter) = {
    var matchChecks = 0
    for (task <- tasks.taskList) {
      for (i <- task.firstBorderStart until task.firstBorderEnd) {
        for (j <- task.secondBorderStart until task.secondBorderEnd) {
          val ref1 = task.tuplesInNodeAsIndexedSeq(i)
          val ref2 = task.tuplesInNodeAsIndexedSeq(j)
          if (i < j && (!tupleToNonWcTransitions.isDefined || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t)))) {
            serializeIfMatch(ref1, ref2, pr, toGeneralEdgeFunction)
          }
          matchChecks += 1
        }
      }
    }
    serializeMatchChecks(matchChecks)
  }

  def serializeMatchChecks(matchChecks: Int) = {
    if (matchChecks != 0) {
      val (prStats, fnameStats) = ConcurrentRoleTreeTraverser.getOrCreateNewStatsPrintWriter(GLOBAL_CONFIG.INDEXING_STATS_RESULT_DIR)
      prStats.println(matchChecks)
      ConcurrentRoleTreeTraverser.releaseStatPrintWriter(prStats, fnameStats)
    }
  }

  def startProcessIntervalsFromBipariteList(tasks:BipartitePairwiseTaskList,
                                            resultDir: File,
                                            context: ExecutionContextExecutor,
                                            processName: String,
                                            futures: ConcurrentHashMap[String, Future[Any]],
                                            toGeneralEdgeFunction: (RoleReference, RoleReference) => RoleMatchCandidate,
                                            tupleToNonWcTransitions: Option[Map[RoleReference, Set[ValueTransition]]]) = {
    val f = Future{
      val (pr,fname) = ConcurrentRoleTreeTraverser.getOrCreateNewPrintWriter(resultDir)
      doPairwiseMatchingBipartiteList(tasks, toGeneralEdgeFunction, tupleToNonWcTransitions, pr)
      ConcurrentRoleTreeTraverser.releasePrintWriter(pr,fname)
    }(context)
    ConcurrentRoleTreeTraverser.setupFuture(f,processName,futures,context)
  }

  def doPairwiseMatchingBipartiteList(tasks: BipartitePairwiseTaskList, toGeneralEdgeFunction: (RoleReference, RoleReference) => RoleMatchCandidate, tupleToNonWcTransitions: Option[Map[RoleReference, Set[ValueTransition]]], pr: PrintWriter) = {
    var matchChecks = 0
    for (task <- tasks.taskList) {
      for (i <- task.firstBorderStart until task.firstBorderEnd) {
        for (j <- task.secondBorderStart until task.secondBorderEnd) {
          val ref1 = task.tuplesLeft(i)
          val ref2 = task.tuplesRight(j)
          if (!tupleToNonWcTransitions.isDefined || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t))) {
            serializeIfMatch(ref1, ref2, pr, toGeneralEdgeFunction)
          }
          matchChecks += 1
        }
      }
    }
    serializeMatchChecks(matchChecks)
  }

  var thresholdForFork = 2000
  var maxPairwiseListSizeForSingleThread = 30

}
