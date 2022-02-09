package de.hpi.wikipedia_data_preparation.compatiblity_graph

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.{AbstractAsynchronousRoleTree, ConcurrentCompatiblityGraphCreator}
import de.hpi.role_matching.cbrm.compatibility_graph.{CompatibilityGraphCreationConfig, GraphConfig}
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, RoleReference}
import de.hpi.wikipedia_data_preparation.original_infobox_data.InfoboxRevisionHistory
import de.hpi.wikipedia_data_preparation.transformed.WikipediaRoleLineage

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.util.regex.Pattern

object CompatibilityGraphByTemplateCreationMain extends App with StrictLogging {
  logger.debug(s"called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_START = InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP
  GLOBAL_CONFIG.STANDARD_TIME_FRAME_END = InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP
  val templates = args(0).split(Pattern.quote(",")).toIndexedSeq
  val byTemplateDir = new File(args(1))
  val resultRootDir = args(2)
  val resultDirEdges = new File(resultRootDir + "/edges/")
  val resultDirStats = new File(resultRootDir + "/stats/")
  val resultDirTime = new File(resultRootDir + "/executionTime/")
  val endDateTrainPhase = LocalDate.parse(args(3))
  val timestampResolutionInDays = args(4).toInt
  val nthreads = args(5).toInt
  val thresholdForFork = args(6).toInt
  val maxPairwiseListSizeForSingleThread = args(7).toInt
  val roleSamplingRate = args(8).toDouble
  val timestampSamplingRate = args(9).toDouble
  val dsName = args(10)
  Seq(resultDirEdges,resultDirStats,resultDirTime,byTemplateDir).foreach(_.mkdirs())
  private val config: CompatibilityGraphCreationConfig = CompatibilityGraphCreationConfig(roleSamplingRate, timestampSamplingRate, 50)
  GLOBAL_CONFIG.INDEXING_CONFIG=config
  AbstractAsynchronousRoleTree.thresholdForFork = thresholdForFork
  AbstractAsynchronousRoleTree.maxPairwiseListSizeForSingleThread = maxPairwiseListSizeForSingleThread
  GLOBAL_CONFIG.trainTimeEnd = endDateTrainPhase
  GLOBAL_CONFIG.granularityInDays = timestampResolutionInDays
  GLOBAL_CONFIG.INDEXING_STATS_RESULT_DIR = resultDirStats
  resultDirStats.mkdir()
  InfoboxRevisionHistory.setGranularityInDays(timestampResolutionInDays)
  val infoboxHistoryFiles = templates.map(t => new File(byTemplateDir.getAbsolutePath + s"/$t.json"))
  val lineagesComplete = infoboxHistoryFiles.flatMap(f => {
    logger.debug(s"Loading lineages in $f")
    WikipediaRoleLineage.fromJsonObjectPerLineFile(f.getAbsolutePath)
  })
  val lineagesTrain = lineagesComplete
    .map(h => h.projectToTimeRange(InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP, endDateTrainPhase))

  val references = RoleLineageWithID.toReferences(lineagesTrain.map(_.toIdentifiedFactLineage))

  val graphConfig = GraphConfig(0, InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP, endDateTrainPhase)
  logger.debug("Starting compatibility graph creation")

  def toGeneralEdgeFunction(a: RoleReference, b: RoleReference) = {
    SimpleCompatbilityGraphEdge(lineagesComplete(a.rowIndex).toIdentifiedFactLineage,lineagesComplete(b.rowIndex).toIdentifiedFactLineage)
  }

  val timeNow = System.currentTimeMillis()
  new ConcurrentCompatiblityGraphCreator(references,
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
  //
  //  val lines = Source.fromFile(resultDirEdges.getAbsolutePath + "/partition_0.json")
  //    .getLines()
  //    .toIndexedSeq
  //    .zipWithIndex
  //    .foreach(t => {
  //      println(t._2)
  //      GeneralEdge.fromJsonString(t._1)
  //    })
}
