package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.internal.InternalFactMatchGraphCreator
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.wikipedia.infobox.original.InfoboxRevisionHistory
import de.hpi.tfm.data.wikipedia.infobox.query.WikipediaInfoboxValueHistoryMatch
import de.hpi.tfm.data.wikipedia.infobox.statistics.edge.EdgeAnalyser
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.IOService

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.util.regex.Pattern

object FactMergingByTemplateMain extends App with StrictLogging{
  IOService.STANDARD_TIME_FRAME_START = InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP
  IOService.STANDARD_TIME_FRAME_END = InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP
  val templates = args(0).split(Pattern.quote(";")).toIndexedSeq
  val templateSetString = templates.mkString("&")
  val byTemplateDir = new File(args(1))
  val resultFileEdges = new File(args(2))
  val resultFileStats = new File(args(3))
  val endDateTrainPhase = LocalDate.parse(args(4))
  val timestampResolutionInDays = args(5).toInt
  GLOBAL_CONFIG.trainTimeEnd=endDateTrainPhase
  GLOBAL_CONFIG.granularityInDays=timestampResolutionInDays
  InfoboxRevisionHistory.setGranularityInDays(timestampResolutionInDays)
  val infoboxHistoryFiles = templates.map(t => new File(byTemplateDir.getAbsolutePath + s"/$t.json"))
  val lineagesComplete = infoboxHistoryFiles.flatMap(f => {
    logger.debug(s"Loading lineages in $f")
    WikipediaInfoboxValueHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
  })
  val lineagesTrain = lineagesComplete
    .map(h => h.projectToTimeRange(InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP,endDateTrainPhase))
  val id = new AssociationIdentifier("wikipedia", templateSetString, 0, Some(0))
  val attrID = 0
  val table = WikipediaInfoboxValueHistory.toAssociationTable(lineagesTrain, id, attrID)
  val graphConfig = GraphConfig(0, InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP, endDateTrainPhase)
  logger.debug("Starting compatibility graph creation")
  val nonInformativeValues:Set[Any] = Set("")
  val edges = new InternalFactMatchGraphCreator(table.tupleReferences, graphConfig,true,nonInformativeValues)
    .toFieldLineageMergeabilityGraph(false)
    .edges
    .map(e => WikipediaInfoboxValueHistoryMatch(lineagesComplete(e.tupleReferenceA.rowIndex), lineagesComplete(e.tupleReferenceB.rowIndex)))
  logger.debug(s"Finished compatibility graph creation, found ${edges.size} edges")
  logger.debug(s"serializing edges to ${resultFileEdges.getAbsolutePath}")
  val writer = new PrintWriter(resultFileEdges.getAbsolutePath)
  edges.foreach(m => m.appendToWriter(writer, false, true))
  writer.close()
  new EdgeAnalyser(edges.map(_.toGeneralEdge),graphConfig,timestampResolutionInDays).toCsvFile(resultFileStats)
}
