package de.hpi.role_matching.compatibility.graph.creation.internal

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.{GLOBAL_CONFIG, IndexingConfig}
import de.hpi.role_matching.compatibility.GraphConfig
import de.hpi.role_matching.compatibility.graph.creation.{FactMatchCreator, TupleReference}
import de.hpi.role_matching.compatibility.graph.representation.simple.GeneralEdge
import de.hpi.role_matching.compatibility.graph.representation.slim.VertexLookupMap
import de.hpi.socrata.tfmp_input.association.AssociationIdentifier
import de.hpi.socrata.tfmp_input.table.nonSketch.FactLineage
import de.hpi.wikipedia.data.compatiblity_graph.CompatibilityGraphByTemplateCreationMain.{args, endDateTrainPhase, lineagesComplete, maxPairwiseListSizeForSingleThread, resultDirEdges, resultDirStats, resultDirTime, roleSamplingRate, thresholdForFork, timestampResolutionInDays, timestampSamplingRate}
import de.hpi.wikipedia.data.compatiblity_graph.WikipediaInfoboxValueHistoryMatch
import de.hpi.wikipedia.data.original.InfoboxRevisionHistory

import java.io.{File, PrintWriter}
import java.time.LocalDate

object InternalFactMatchGraphCreationMain extends App with StrictLogging{
  GLOBAL_CONFIG.setDatesForDataSource("socrata")
  val dataDir = args(0)
  val resultDirEdges = new File(args(1))
  val resultDirStats = new File(args(2))
  val resultDirTime = new File(args(3))
  val endDateTrainPhase = LocalDate.parse(args(4))
  val timestampResolutionInDays = args(5).toInt
  val nthreads = args(6).toInt
  val thresholdForFork = args(7).toInt
  val maxPairwiseListSizeForSingleThread = args(8).toInt
  val roleSamplingRate = args(9).toDouble
  val timestampSamplingRate = args(10).toDouble
  val dsName = args(11)
  Seq(resultDirEdges,resultDirStats,resultDirTime).foreach(_.mkdirs())
  private val config: IndexingConfig = IndexingConfig(roleSamplingRate, timestampSamplingRate, 50)
  GLOBAL_CONFIG.INDEXING_CONFIG=config
  FactMatchCreator.thresholdForFork = thresholdForFork
  FactMatchCreator.maxPairwiseListSizeForSingleThread = maxPairwiseListSizeForSingleThread
  GLOBAL_CONFIG.trainTimeEnd = endDateTrainPhase
  GLOBAL_CONFIG.granularityInDays = timestampResolutionInDays
  GLOBAL_CONFIG.INDEXING_STATS_RESULT_DIR = resultDirStats
  resultDirStats.mkdir()
  InfoboxRevisionHistory.setGranularityInDays(timestampResolutionInDays)
  val identifiedLineages = VertexLookupMap.fromJsonFile(new File(dataDir + s"/$dsName.json").getAbsolutePath)
    .posToLineage
    .toIndexedSeq
    .sortBy(_._1)
  val lineages=identifiedLineages
    .map(_._2.factLineage.toFactLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,endDateTrainPhase))
  val id = new AssociationIdentifier(dsName, "all", 0, Some(0))
  val attrID = 0
  val table = FactLineage.toAssociationTable(lineages,id,attrID)
  val timeNow = System.currentTimeMillis()
  val graphConfig = GraphConfig(1: Int, GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, endDateTrainPhase)

  def toGeneralEdgeFunction(a: TupleReference[Any], b: TupleReference[Any]) = {
    GeneralEdge(identifiedLineages(a.rowIndex)._2,identifiedLineages(b.rowIndex)._2)
  }

  new ConcurrentMatchGraphCreator(table.tupleReferences,
    graphConfig,
    true,
    GLOBAL_CONFIG.nonInformativeValues,
    nthreads,
    resultDirEdges,
    toGeneralEdgeFunction)
  val timeAfter = System.currentTimeMillis()
  val timeInSeconds = (timeAfter - timeNow) / 1000.0

  def getFileName = s"/${dsName}_nThreads_${nthreads}_${config.samplingRateRoles}_${config.samplingRateTimestamps}.csv"

  val pr = new PrintWriter(resultDirTime+ getFileName)
  pr.println("NThreads,RolesampleRate,TimestampSampleRate,Time [s]")
  pr.println(s"$nthreads,${config.samplingRateRoles},${config.samplingRateTimestamps},$timeInSeconds")
  pr.close()
  logger.debug(s"Finished in time $timeAfter")
  private val edgeFiles: Array[File] = resultDirEdges.listFiles()
  logger.debug(s"Finished compatibility graph creation, found ${edgeFiles.size} edge files")

}
