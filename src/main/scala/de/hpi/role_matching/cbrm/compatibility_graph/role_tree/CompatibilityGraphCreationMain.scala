package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.compatibility_graph.{CompatibilityGraphCreationConfig, GraphConfig}
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, RoleReference, Roleset}
import de.hpi.wikipedia_data_preparation.original_infobox_data.InfoboxRevisionHistory

import java.io.{File, PrintWriter}
import java.time.LocalDate

object CompatibilityGraphCreationMain extends App with StrictLogging {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val rolsetFile = args(1)
  val resultDirEdges = new File(args(2))
  val resultDirStats = new File(args(3))
  val resultDirTime = new File(args(4))
  val endDateTrainPhase = LocalDate.parse(args(5))
  val nthreads = args(6).toInt
  val thresholdForFork = args(7).toInt
  val maxPairwiseListSizeForSingleThread = args(8).toInt
  val roleSamplingRate = args(9).toDouble
  val timestampSamplingRate = args(10).toDouble
  Seq(resultDirEdges, resultDirStats, resultDirTime).foreach(_.mkdirs())
  private val config: CompatibilityGraphCreationConfig = CompatibilityGraphCreationConfig(roleSamplingRate, timestampSamplingRate, 50)
  GLOBAL_CONFIG.INDEXING_CONFIG = config
  AbstractAsynchronousRoleTree.thresholdForFork = thresholdForFork
  AbstractAsynchronousRoleTree.maxPairwiseListSizeForSingleThread = maxPairwiseListSizeForSingleThread
  GLOBAL_CONFIG.trainTimeEnd = endDateTrainPhase
  GLOBAL_CONFIG.INDEXING_STATS_RESULT_DIR = resultDirStats
  resultDirStats.mkdir()
  val identifiedLineages = Roleset.fromJsonFile(new File(rolsetFile).getAbsolutePath)
    .positionToRoleLineage
    .toIndexedSeq
    .sortBy(_._1)
  val lineages = identifiedLineages
    .map(t => {
      val newLineage = t._2.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, endDateTrainPhase)
      RoleLineageWithID(t._2.id,newLineage.toSerializationHelper)
    })
  val timeNow = System.currentTimeMillis()
  val graphConfig = GraphConfig(1: Int, GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, endDateTrainPhase)

  val references = RoleLineageWithID.toReferences(lineages)

  def toGeneralEdgeFunction(a: RoleReference, b: RoleReference) = {
    SimpleCompatbilityGraphEdge(identifiedLineages(a.rowIndex)._2, identifiedLineages(b.rowIndex)._2)
  }

  new ConcurrentCompatiblityGraphCreator(references,
    graphConfig,
    true,
    GLOBAL_CONFIG.nonInformativeValues,
    nthreads,
    resultDirEdges,
    toGeneralEdgeFunction)
  val timeAfter = System.currentTimeMillis()
  val timeInSeconds = (timeAfter - timeNow) / 1000.0

  val pr = new PrintWriter(resultDirTime + "/runtime.csv")
  pr.println("NThreads,RolesampleRate,TimestampSampleRate,Time [s]")
  pr.println(s"$nthreads,${config.samplingRateRoles},${config.samplingRateTimestamps},$timeInSeconds")
  pr.close()
  logger.debug(s"Finished in time $timeAfter")
  private val edgeFiles: Array[File] = resultDirEdges.listFiles()
  logger.debug(s"Finished compatibility graph creation, found ${edgeFiles.size} edge files")

}
