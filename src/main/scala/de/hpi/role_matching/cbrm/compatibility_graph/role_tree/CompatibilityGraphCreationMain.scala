package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import com.typesafe.scalalogging.StrictLogging
import de.hpi.data_preparation.socrata.tfmp_input.association.AssociationIdentifier
import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch.FactLineage
import de.hpi.data_preparation.wikipedia.data.original.InfoboxRevisionHistory
import de.hpi.role_matching.cbrm.compatibility_graph.{GraphConfig, CompatibilityGraphCreationConfig}
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.Roleset
import de.hpi.role_matching.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate

object CompatibilityGraphCreationMain extends App with StrictLogging {
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
  Seq(resultDirEdges, resultDirStats, resultDirTime).foreach(_.mkdirs())
  private val config: CompatibilityGraphCreationConfig = CompatibilityGraphCreationConfig(roleSamplingRate, timestampSamplingRate, 50)
  GLOBAL_CONFIG.INDEXING_CONFIG = config
  AbstractAsynchronousRoleTree.thresholdForFork = thresholdForFork
  AbstractAsynchronousRoleTree.maxPairwiseListSizeForSingleThread = maxPairwiseListSizeForSingleThread
  GLOBAL_CONFIG.trainTimeEnd = endDateTrainPhase
  GLOBAL_CONFIG.granularityInDays = timestampResolutionInDays
  GLOBAL_CONFIG.INDEXING_STATS_RESULT_DIR = resultDirStats
  resultDirStats.mkdir()
  InfoboxRevisionHistory.setGranularityInDays(timestampResolutionInDays)
  val identifiedLineages = Roleset.fromJsonFile(new File(dataDir + s"/$dsName.json").getAbsolutePath)
    .positionToRoleLineage
    .toIndexedSeq
    .sortBy(_._1)
  val lineages = identifiedLineages
    .map(_._2.factLineage.toFactLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, endDateTrainPhase))
  val id = new AssociationIdentifier(dsName, "all", 0, Some(0))
  val attrID = 0
  val table = FactLineage.toAssociationTable(lineages, id, attrID)
  val timeNow = System.currentTimeMillis()
  val graphConfig = GraphConfig(1: Int, GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, endDateTrainPhase)

  def toGeneralEdgeFunction(a: RoleReference[Any], b: RoleReference[Any]) = {
    SimpleCompatbilityGraphEdge(identifiedLineages(a.rowIndex)._2, identifiedLineages(b.rowIndex)._2)
  }

  new ConcurrentCompatiblityGraphCreator(table.tupleReferences,
    graphConfig,
    true,
    GLOBAL_CONFIG.nonInformativeValues,
    nthreads,
    resultDirEdges,
    toGeneralEdgeFunction)
  val timeAfter = System.currentTimeMillis()
  val timeInSeconds = (timeAfter - timeNow) / 1000.0

  def getFileName = s"/${dsName}_nThreads_${nthreads}_${config.samplingRateRoles}_${config.samplingRateTimestamps}.csv"

  val pr = new PrintWriter(resultDirTime + getFileName)
  pr.println("NThreads,RolesampleRate,TimestampSampleRate,Time [s]")
  pr.println(s"$nthreads,${config.samplingRateRoles},${config.samplingRateTimestamps},$timeInSeconds")
  pr.close()
  logger.debug(s"Finished in time $timeAfter")
  private val edgeFiles: Array[File] = resultDirEdges.listFiles()
  logger.debug(s"Finished compatibility graph creation, found ${edgeFiles.size} edge files")

}
