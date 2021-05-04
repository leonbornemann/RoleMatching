package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.internal.InternalFactMatchGraphCreator
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.wikipedia.infobox.original.InfoboxRevisionHistory
import de.hpi.tfm.data.wikipedia.infobox.query.WikipediaInfoboxValueHistoryMatch
import de.hpi.tfm.data.wikipedia.infobox.statistics.edge.EdgeAnalyser
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory
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
  val resultDir = new File(args(2))
  val endDateTrainPhase = LocalDate.parse(args(3))
  val timestampResolutionInDays = args(4).toInt
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
  logger.debug("Finished compatibility graph creation")
  val writer = new PrintWriter(resultDir.getAbsolutePath + s"/${templateSetString}_edges.json")
  edges.foreach(m => m.appendToWriter(writer, false, true))
  writer.close()
  new EdgeAnalyser(edges,graphConfig,timestampResolutionInDays).toCsvFile(new File(resultDir.getAbsolutePath + s"/${templateSetString}_edgeStats.csv"))
}
