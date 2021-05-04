package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.wikipedia.infobox.original.InfoboxRevisionHistory
import de.hpi.tfm.data.wikipedia.infobox.query.WikipediaInfoboxValueHistoryMatch
import de.hpi.tfm.io.IOService

import java.io.{File, PrintWriter}
import java.time.LocalDate

object FactFilteringMain extends App {
  IOService.STANDARD_TIME_FRAME_START = InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP
  IOService.STANDARD_TIME_FRAME_END = InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP
  val matchFile = new File(args(0))
  val resultFile = new PrintWriter(new File(args(1)))
  val endDateTrainPhase = LocalDate.parse(args(2))
  val timestampResolutionInDays = args(3).toInt
  val graphConfig = GraphConfig(0, InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP, endDateTrainPhase)
  InfoboxRevisionHistory.setGranularityInDays(timestampResolutionInDays)
  val edges = WikipediaInfoboxValueHistoryMatch.fromJsonObjectPerLineFile(matchFile.getAbsolutePath)
  val filtered = edges.filter(e => {
    val statRow = e.toWikipediaEdgeStatRow(graphConfig,timestampResolutionInDays).toGeneralStatRow
    !statRow.remainsValid && !statRow.isInteresting
  })
  println(s"Found ${filtered.size} weird edges")
  filtered.foreach(f => f.appendToWriter(resultFile,false,true))
  resultFile.close()
}
