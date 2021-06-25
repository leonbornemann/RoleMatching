package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.evaluation.data.{GeneralEdge, IdentifiedFactLineage, SLimGraph, SlimGraphWithoutWeight}
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.fact_merging.metrics.{MultipleEventWeightScore, TFIDFMapStorage, TFIDFWeightingVariant}
import de.hpi.tfm.io.IOService

import java.io.{File, PrintWriter}
import java.time.LocalDate

object GraphFilterMain extends App with StrictLogging {
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val matchFile = new File(args(0))
  val resultFile = new File(args(1))
  val timeStart = LocalDate.parse(args(2))
  val smallestTrainTimeEnd = LocalDate.parse(args(3))
  val endDateTrainPhases = args(4).split(";").map(LocalDate.parse(_)).toIndexedSeq
  val timeEnd = LocalDate.parse(args(5))
  IOService.STANDARD_TIME_FRAME_START = timeStart
  IOService.STANDARD_TIME_FRAME_END = timeEnd
  val edges = GeneralEdge.iterableFromJsonObjectPerLineFile(matchFile.getAbsolutePath)
  val slimGraph = SlimGraphWithoutWeight.fromGeneralEdgeIterator(edges,timeStart,smallestTrainTimeEnd,endDateTrainPhases)
  slimGraph.toJsonFile(resultFile)
  logger.debug("Done with Filtered Graphs")

}
