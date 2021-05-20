package de.hpi.tfm.fact_merging.optimization

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.evaluation.data.{GeneralEdge, SlimOptimizationGraph}
import de.hpi.tfm.fact_merging.metrics.{EdgeScore, MultipleEventWeightScore}
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.TransitionHistogramMode.TransitionHistogramMode
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.{RuzickaSimilarity, TransitionHistogramMode}
import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate

object GraphOptimizationMain extends App with StrictLogging{
  val edgeFile = args(0)
  val TIMESTAMP_RESOLUTION_IN_DAYS = args(1).toInt
  val timeStart = LocalDate.parse(args(2))
  val trainTimeEnd = LocalDate.parse(args(3))
  val timeEnd = LocalDate.parse(args(4))
  val metricThreshold = args(5).toDouble
  val slimGraphFile = new File(args(6))
  val resultFile = new File(args(7)) // get real path somewhere!
  assert(resultFile.getParentFile.exists())
  IOService.STANDARD_TIME_FRAME_START=timeStart
  IOService.STANDARD_TIME_FRAME_END=timeEnd
  val distanceMetric:EdgeScore[Any] = ???//new MultipleEventWeightScore(TIMESTAMP_RESOLUTION_IN_DAYS,trainTimeEnd)
  logger.debug("Loading edges")
  var edges = GeneralEdge.fromJsonObjectPerLineFile(args(0)).toIndexedSeq
  logger.debug("Finished loading edges")
  //val histogramMode: TransitionHistogramMode = TransitionHistogramMode.COUNT_NON_CHANGE_ONLY_ONCE //get these as params!
  val slimGraph = SlimOptimizationGraph.fromIdentifiedEdges(edges,distanceMetric) //or load this directly
  slimGraph.toJsonFile(slimGraphFile)
  edges=null
  logger.debug("Finished transformation to slim graph")
  val optimizer = new GreedyEdgeBasedOptimizer(slimGraph.transformToOptimizationGraph, resultFile, metricThreshold) //TODO: get threshold somewere!
  optimizer.runComponentWiseOptimization()
}
