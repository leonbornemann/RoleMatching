package de.hpi.tfm.compatibility.graph.fact

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, ValueTransition}
import de.hpi.tfm.evaluation.data.GeneralEdge

import java.io.{File, PrintWriter}
import java.time.temporal.TemporalField
import java.util.UUID
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

abstract class FactMatchCreator[A](val parallelBatchExecutor:ParallelBatchExecutor[A]) extends StrictLogging{

  var curCandidateBuffer = scala.collection.mutable.ArrayBuffer[(TupleReference[A],TupleReference[A])]()
  val batchSize = 10000

  def getGraphConfig: GraphConfig

}
