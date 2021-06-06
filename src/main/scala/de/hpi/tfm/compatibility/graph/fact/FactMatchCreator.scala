package de.hpi.tfm.compatibility.graph.fact

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.internal.InternalFactMatchGraphCreator
import de.hpi.tfm.compatibility.graph.fact.internal.InternalFactMatchGraphCreator.logger
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, ValueTransition}
import de.hpi.tfm.evaluation.data.GeneralEdge

import java.io.{File, PrintWriter}
import java.time.temporal.TemporalField
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

abstract class FactMatchCreator[A](val toGeneralEdgeFunction:((TupleReference[A],TupleReference[A]) => GeneralEdge),
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


  def thresholdForFork = FactMatchCreator.thresholdForFork

  if(!isAsynch && !prOption.isDefined){
    logger.debug("That is weird - we are probably overwriting an existing file")
    assert(false)
  }

  var fnameOfWriter:Option[String] = None

  val pr = if(prOption.isDefined)
    prOption.get else {
      val (writer,fname) = ConcurrentMatchGraphCreator.getOrCreateNewPrintWriter(resultDir)
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
      ConcurrentMatchGraphCreator.releasePrintWriter(pr,fnameOfWriter.get)
      //pr.close()
  }

  def execute():Unit

  def getGraphConfig: GraphConfig

  def serializeIfMatch(tr1:TupleReference[A],tr2:TupleReference[A],pr:PrintWriter) = {
    FactMatchCreator.serializeIfMatch(tr1,tr2,pr,toGeneralEdgeFunction)
  }

  def partitionToIntervals(inputList: IndexedSeq[TupleReference[A]], border: Int) = {
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
object FactMatchCreator {

  def getTupleMatchOption[A](ref1:TupleReference[A], ref2:TupleReference[A]) = {
    val left = ref1.getDataTuple.head
    val right = ref2.getDataTuple.head // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
    val evidence = left.getOverlapEvidenceCount(right)
    if (evidence == -1) {
      None
    } else {
      Some(FactMatch(ref1,ref2, evidence))
    }
  }

  def serializeIfMatch[A](tr1:TupleReference[A],tr2:TupleReference[A],pr:PrintWriter,toGeneralEdgeFunction:((TupleReference[A],TupleReference[A]) => GeneralEdge)) = {
    val option = getTupleMatchOption(tr1,tr2)
    if(option.isDefined){
      val e = option.get
      val edge = toGeneralEdgeFunction(e.tupleReferenceA,e.tupleReferenceB)
      edge.appendToWriter(pr,false,true)
    }
  }

  //end borders are exclusive
  def startProcessIntervalsFromSameList[A](tuplesInNodeAsIndexedSeq: IndexedSeq[TupleReference[A]],
                                           i1: (Int, Int),
                                           i2: (Int, Int),
                                           resultDir:File,
                                           context:ExecutionContextExecutor,
                                           processName:String,
                                           futures:java.util.concurrent.ConcurrentHashMap[String,Future[Any]],
                                           toGeneralEdgeFunction:((TupleReference[A],TupleReference[A]) => GeneralEdge),
                                           tupleToNonWcTransitions:Option[Map[TupleReference[A], Set[ValueTransition[A]]]]
                                          ) = {
    val f = Future{
      val (pr,fname) = ConcurrentMatchGraphCreator.getOrCreateNewPrintWriter(resultDir)
      val (firstBorderStart,firstBorderEnd) = i1
      val (secondBorderStart,secondBorderEnd) = i2
      for(i <- firstBorderStart until firstBorderEnd){
        for(j <- secondBorderStart until secondBorderEnd){
          val ref1 = tuplesInNodeAsIndexedSeq(i)
          val ref2 = tuplesInNodeAsIndexedSeq(j)
          if(i<j && (!tupleToNonWcTransitions.isDefined || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t)))){
            serializeIfMatch(ref1,ref2,pr,toGeneralEdgeFunction)
          }
        }
      }
      ConcurrentMatchGraphCreator.releasePrintWriter(pr,fname)
    }(context)
    ConcurrentMatchGraphCreator.setupFuture(f,processName,futures,context)
  }

  def startProcessIntervalsFromBipariteList[A](tuplesLeft: IndexedSeq[TupleReference[A]], tuplesRight: IndexedSeq[TupleReference[A]], i1: (Int, Int), i2: (Int, Int), resultDir: File, context: ExecutionContextExecutor, processName: String, futures: ConcurrentHashMap[String, Future[Any]], toGeneralEdgeFunction: (TupleReference[A], TupleReference[A]) => GeneralEdge, tupleToNonWcTransitions: Option[Map[TupleReference[A], Set[ValueTransition[A]]]]) = {
    val f = Future{
      val (pr,fname) = ConcurrentMatchGraphCreator.getOrCreateNewPrintWriter(resultDir)
      val (firstBorderStart,firstBorderEnd) = i1
      val (secondBorderStart,secondBorderEnd) = i2
      for(i <- firstBorderStart until firstBorderEnd){
        for(j <- secondBorderStart until secondBorderEnd){
          val ref1 = tuplesLeft(i)
          val ref2 = tuplesRight(j)
          if(!tupleToNonWcTransitions.isDefined || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t))){
            serializeIfMatch(ref1,ref2,pr,toGeneralEdgeFunction)
          }
        }
      }
      ConcurrentMatchGraphCreator.releasePrintWriter(pr,fname)
    }(context)
    ConcurrentMatchGraphCreator.setupFuture(f,processName,futures,context)
  }


  var thresholdForFork = 2000
  var maxPairwiseListSizeForSingleThread = 30

}
