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
                                   val fname:String,
                                   prOption:Option[PrintWriter],
                                   isAsynch:Boolean=true,
                                   externalRecurseDepth:Int
                                  ) extends StrictLogging{

  var totalNumTopLevelNodes = 0
  var processedTopLvlNodes = 0

  def logProgress: Boolean = externalRecurseDepth==0 && totalNumTopLevelNodes > 1000 && (totalNumTopLevelNodes / processedTopLvlNodes) % (totalNumTopLevelNodes / 1000)==0

  def maybeLogProgress() = {
    if(logProgress)
      logger.debug(s"Root Process finished ${100 * processedTopLvlNodes / totalNumTopLevelNodes.toDouble}% of top-lvl nodes")
  }

  def isRootProcess: Boolean = externalRecurseDepth==0


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

  val mySubNodeFutures = scala.collection.mutable.HashMap[String,Future[FactMatchCreator[A]]]()
  var parallelRecurseCounter = 0
  var internalRecurseCounter = 0
  init()

  def init() = {
    if(isAsynch)
      logger.debug(s"Created new Asynchronously running process $fname")
    execute()
    if(prOption.isEmpty)
      ConcurrentMatchGraphCreator.releasePrintWriter(pr,fnameOfWriter.get)
      //pr.close()
  }

  def execute():Unit

  def getGraphConfig: GraphConfig

  def getTupleMatchOption(ref1:TupleReference[A], ref2:TupleReference[A]) = {
    val left = ref1.getDataTuple.head
    val right = ref2.getDataTuple.head // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
    val evidence = left.getOverlapEvidenceCount(right)
    if (evidence == -1) {
      None
    } else {
      Some(FactMatch(ref1,ref2, evidence))
    }
  }

  def serializeIfMatch(tr1:TupleReference[A],tr2:TupleReference[A],pr:PrintWriter) = {
    val option = getTupleMatchOption(tr1,tr2)
    if(option.isDefined){
      val e = option.get
      val edge = toGeneralEdgeFunction(e.tupleReferenceA,e.tupleReferenceB)
      edge.appendToWriter(pr,false,true)
    }
  }
}
object FactMatchCreator {
  var thresholdForFork = 2000

}
