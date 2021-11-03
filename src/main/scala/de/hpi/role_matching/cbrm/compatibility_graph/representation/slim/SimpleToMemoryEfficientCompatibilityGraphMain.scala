package de.hpi.role_matching.cbrm.compatibility_graph.representation.slim

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge

import java.io.File
import java.time.LocalDate

/**
 * Transforms the simple but memory inefficient variant of compatiblity graphs to the memory efficient one
 */
object SimpleToMemoryEfficientCompatibilityGraphMain extends App with StrictLogging {
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val matchFile = new File(args(0))
  val resultFile = new File(args(1))
  val timeStart = LocalDate.parse(args(2))
  val smallestTrainTimeEnd = LocalDate.parse(args(3))
  val endDateTrainPhases = args(4).split(";").map(LocalDate.parse(_)).toIndexedSeq
  val timeEnd = LocalDate.parse(args(5))
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_START = timeStart
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_END = timeEnd
  val edges = SimpleCompatbilityGraphEdge.iterableFromJsonObjectPerLineFile(matchFile.getAbsolutePath)
  val slimGraph = MemoryEfficientCompatiblityGraphWithoutEdgeWeight.fromGeneralEdgeIterator(edges, timeStart, smallestTrainTimeEnd, endDateTrainPhases)
  slimGraph.toJsonFile(resultFile)
  logger.debug("Done with Filtered Graphs")

}
