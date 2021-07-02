package de.hpi.role_matching.evaluation.edge

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.compatibility.GraphConfig
import de.hpi.role_matching.compatibility.graph.representation.simple.GeneralEdge
import de.hpi.role_matching.scoring.TFIDFMapStorage
import de.hpi.wikipedia.data.compatiblity_graph.WikipediaInfoboxValueHistoryMatch
import de.hpi.wikipedia.data.original.InfoboxRevisionHistory

import java.io.File
import java.time.LocalDate

object EdgeAnalysisMain extends App with StrictLogging {
  logger.debug(s"called with ${args.toIndexedSeq}")
  val matchFile = new File(args(0))
  val resultFile = new File(args(1))
  val timeStart = LocalDate.parse(args(2))
  val endDateTrainPhase = LocalDate.parse(args(3))
  val timeEnd = LocalDate.parse(args(4))
  val timestampResolutionInDays = args(5).toInt
  val edgeType = args(6)
  val tfIDFFile = if (args.size == 8) Some(args(7)) else None
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_START = timeStart
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_END = timeEnd
  InfoboxRevisionHistory.setGranularityInDays(timestampResolutionInDays)
  val graphConfig = GraphConfig(0, timeStart, endDateTrainPhase)
  logger.debug("Beginning to load TF-IDF File")
  val tfIDF = tfIDFFile.map(f => TFIDFMapStorage.fromJsonFile(f))
  logger.debug("Beginning to load edges")
  var edges: collection.Seq[GeneralEdge] = IndexedSeq()
  if (edgeType == "general") {
    edges = GeneralEdge.fromJsonObjectPerLineFile(matchFile.getAbsolutePath)
  } else if (edgeType == "wikipedia") {
    edges = WikipediaInfoboxValueHistoryMatch.fromJsonObjectPerLineFile(matchFile.getAbsolutePath)
      .map(_.toGeneralEdge)
  } else {
    assert(false)
  }
  //logger.debug(s"Found ${edges.size} edges of which ${edges.filter(_.toGeneralEdgeStatRow(timestampResolutionInDays,graphConfig).remainsValid).size} remain valid")
  //  edges
  //    .filter(_.toWikipediaEdgeStatRow(graphConfig,timestampResolutionInDays).toGeneralStatRow.remainsValid)
  //    .zipWithIndex
  //    .foreach{case (e,i) => {
  ////      val str = e.a.toWikipediaURLInfo + "===" + e.b.toWikipediaURLInfo
  ////      println(str)
  //      e.printTabularEventLineageString
  //      val generalStatRow = e.toWikipediaEdgeStatRow(graphConfig, timestampResolutionInDays)
  //      println(generalStatRow)
  //      val computer = new RuzickaDistanceComputer(e.a.lineage.toFactLineage,
  //        e.b.lineage.toFactLineage,
  //        1,
  //        TransitionHistogramMode.NORMAL)
  ////      println(e.a.lineage.toFactLineage.toShortString)
  ////      println(e.b.lineage.toFactLineage.toShortString)
  ////      println("-----------------------------------------------------------------------------------------------------------------")
  //      println(computer.computeScore())
  //      println(computer.computeScore())
  //    }}
  logger.debug("Finsihed loading edges")
  new EdgeAnalyser(edges, graphConfig, timestampResolutionInDays, GLOBAL_CONFIG.nonInformativeValues, tfIDF).toCsvFile(resultFile)
}
