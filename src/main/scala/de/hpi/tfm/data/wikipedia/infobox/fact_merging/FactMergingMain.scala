package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.internal.InternalFactMatchGraphCreator
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.wikipedia.infobox.original.InfoboxRevisionHistory
import de.hpi.tfm.data.wikipedia.infobox.query.WikipediaInfoboxValueHistoryMatch
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory
import de.hpi.tfm.io.IOService

import java.io.PrintWriter
import java.time.LocalDate

object FactMergingMain extends App with StrictLogging{
  val vertexFile = args(0)
  val edgeFile = args(1)
  val standardTimeStart = LocalDate.parse(args(2))
  val endDateTrainPhase = LocalDate.parse(args(3))
  val standardTimeEnd = LocalDate.parse(args(4))
  val timestampResolutionInDays = args(5).toInt
  IOService.STANDARD_TIME_FRAME_START=standardTimeStart
  IOService.STANDARD_TIME_FRAME_END=standardTimeEnd
  InfoboxRevisionHistory.setGranularityInDays(timestampResolutionInDays)
  val lineagesComplete = WikipediaInfoboxValueHistory.fromJsonObjectPerLineFile(vertexFile)
    .toIndexedSeq
  val lineagesTrain = lineagesComplete
    .map(h => h.projectToTimeRange(InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP,endDateTrainPhase))
  val id = new AssociationIdentifier("wikipedia", "test", 0, Some(0))
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
  val writer = new PrintWriter(edgeFile)
  edges.foreach(m => m.appendToWriter(writer, false, true))
  writer.close()
}