package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.wikipedia.infobox.original.InfoboxRevisionHistory
import de.hpi.tfm.data.wikipedia.infobox.query.WikipediaInfoboxValueHistoryMatch
import de.hpi.tfm.data.wikipedia.infobox.statistics.edge.EdgeAnalyser
import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate
import java.util.regex.Pattern

object EdgeAnalysisMain extends App with StrictLogging{
  IOService.STANDARD_TIME_FRAME_START = InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP
  IOService.STANDARD_TIME_FRAME_END = InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP
  val matchFile = new File(args(0))
  val resultFile = new File(args(1))
  val endDateTrainPhase = LocalDate.parse(args(2))
  val timestampResolutionInDays = args(3).toInt
  InfoboxRevisionHistory.setGranularityInDays(timestampResolutionInDays)
  val graphConfig = GraphConfig(0, InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP, endDateTrainPhase)
  logger.debug("Beginning to load edges")
  val edges = WikipediaInfoboxValueHistoryMatch.fromJsonObjectPerLineFile(matchFile.getAbsolutePath)
  edges
    .filter(_.toWikipediaEdgeStatRow(graphConfig,timestampResolutionInDays).toGeneralStatRow.remainsValid)
    .zipWithIndex
    .foreach{case (e,i) => {
//      val str = e.a.toWikipediaURLInfo + "===" + e.b.toWikipediaURLInfo
//      println(str)
      e.printTabularEventLineageString
//      println(e.a.lineage.toFactLineage.toShortString)
//      println(e.b.lineage.toFactLineage.toShortString)
//      println("-----------------------------------------------------------------------------------------------------------------")
    }}
  assert(false)
  logger.debug("Finsihed loading edges")
  new EdgeAnalyser(edges,graphConfig,timestampResolutionInDays).toCsvFile(resultFile)
}
