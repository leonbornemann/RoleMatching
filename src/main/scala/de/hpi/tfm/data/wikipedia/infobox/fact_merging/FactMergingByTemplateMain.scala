package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.{ConcurrentMatchGraphCreator, FactMatchCreator, TupleReference}
import de.hpi.tfm.compatibility.graph.fact.internal.InternalFactMatchGraphCreator
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.wikipedia.infobox.original.InfoboxRevisionHistory
import de.hpi.tfm.data.wikipedia.infobox.query.WikipediaInfoboxValueHistoryMatch
import de.hpi.tfm.data.wikipedia.infobox.statistics.edge.EdgeAnalyser
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory
import de.hpi.tfm.evaluation.data.GeneralEdge
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.IOService

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.util.concurrent.Executors
import java.util.regex.Pattern
import scala.concurrent.{ExecutionContext, Future}

object FactMergingByTemplateMain extends App with StrictLogging{
  IOService.STANDARD_TIME_FRAME_START = InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP
  IOService.STANDARD_TIME_FRAME_END = InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP
  val templates = args(0).split(Pattern.quote(";")).toIndexedSeq
  val templateSetString = templates.mkString("&")
  val byTemplateDir = new File(args(1))
  val resultDirEdges = new File(args(2))
  val resultFileStats = new File(args(3))
  val endDateTrainPhase = LocalDate.parse(args(4))
  val timestampResolutionInDays = args(5).toInt
  val nthreads = args(6).toInt
  val thresholdForFork = args(7).toInt
  FactMatchCreator.thresholdForFork = thresholdForFork
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

  def toGeneralEdgeFunction(a:TupleReference[Any],b:TupleReference[Any]) = {
    WikipediaInfoboxValueHistoryMatch(lineagesComplete(a.rowIndex), lineagesComplete(b.rowIndex))
      .toGeneralEdge
  }

  new ConcurrentMatchGraphCreator(table.tupleReferences,
    graphConfig,
    true,
    GLOBAL_CONFIG.nonInformativeValues,
    nthreads,
    resultDirEdges,
    toGeneralEdgeFunction
  )
  private val edgeFiles: Array[File] = resultDirEdges.listFiles()
  logger.debug(s"Finished compatibility graph creation, found ${edgeFiles.size} edge files")
  private val generalEdges: IndexedSeq[GeneralEdge] =  edgeFiles.flatMap(f => {
    GeneralEdge.fromJsonObjectPerLineFile(f.getAbsolutePath)
  })
  new EdgeAnalyser(generalEdges,graphConfig,timestampResolutionInDays,GLOBAL_CONFIG.nonInformativeValues).toCsvFile(resultFileStats)
}
