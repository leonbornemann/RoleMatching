package de.hpi.role_matching.evaluation.tuning

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.data_preparation.wikipedia.data.compatiblity_graph.WikipediaInfoboxValueHistoryMatch
import de.hpi.data_preparation.wikipedia.data.original.InfoboxRevisionHistory
import de.hpi.role_matching.cbrm.evidence_based_weighting.isf.ISFMapStorage

import java.io.File
import java.time.LocalDate

object EdgeStatisticsMain extends App with StrictLogging {
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
  val tfIDF = tfIDFFile.map(f => ISFMapStorage.fromJsonFile(f))
  logger.debug("Beginning to load edges")
  var edges: collection.Seq[SimpleCompatbilityGraphEdge] = IndexedSeq()
  if (edgeType == "general") {
    edges = SimpleCompatbilityGraphEdge.fromJsonObjectPerLineFile(matchFile.getAbsolutePath)
  } else if (edgeType == "wikipedia") {
    edges = WikipediaInfoboxValueHistoryMatch.fromJsonObjectPerLineFile(matchFile.getAbsolutePath)
      .map(_.toGeneralEdge)
  } else {
    assert(false)
  }
  logger.debug("Finsihed loading edges")
  new EdgeStatisticsGatherer(edges, graphConfig, timestampResolutionInDays, GLOBAL_CONFIG.nonInformativeValues, tfIDF).toCsvFile(resultFile)
}
