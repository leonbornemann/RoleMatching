package de.hpi.role_matching.blocking.cbrb

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.blocking.cbrb.role_tree.{AbstractAsynchronousRoleTree, ConcurrentRoleTreeTraverser, RoleTreeExecutionConfig}
import de.hpi.role_matching.data.{RoleLineageWithID, RoleMatchCandidate, RoleReference, Roleset}
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate

object CBRBMain extends App with StrictLogging {
  val timeNow = System.currentTimeMillis()
  println(s"Called with ${args.toIndexedSeq}")
  private val source: String = args(0).split(",")(0)
  private val timeFactor: Int = args(0).split(",")(1).toInt
  GLOBAL_CONFIG.setSettingsForDataSource(source, timeFactor)
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
  val serializeGroupsOnly = args(11).toBoolean
  val filterByCommonTransition = args(12).toBoolean
  Seq(resultDirEdges, resultDirStats, resultDirTime).foreach(_.mkdirs())
  private val config: RoleTreeExecutionConfig = RoleTreeExecutionConfig(roleSamplingRate, timestampSamplingRate, 50)
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
      RoleLineageWithID(t._2.id, newLineage.toSerializationHelper)
    })
  println(lineages.size)
  val graphConfig = CBRBConfig(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, endDateTrainPhase)

  val references = RoleLineageWithID.toReferences(lineages, endDateTrainPhase)

  def toGeneralEdgeFunction(a: RoleReference, b: RoleReference) = {
    RoleMatchCandidate(identifiedLineages(a.rowIndex)._2, identifiedLineages(b.rowIndex)._2)
  }

  new ConcurrentRoleTreeTraverser(references,
    graphConfig,
    filterByCommonTransition,
    GLOBAL_CONFIG.nonInformativeValues,
    nthreads,
    resultDirEdges,
    toGeneralEdgeFunction,
    serializeGroupsOnly)
  val timeAfter = System.currentTimeMillis()
  val timeInSeconds = (timeAfter - timeNow) / 1000.0

  val pr = new PrintWriter(resultDirTime + "/runtime.csv")
  pr.println("NThreads,RolesampleRate,TimestampSampleRate,Time [s]")
  pr.println(s"$nthreads,${config.samplingRateRoles},${config.samplingRateTimestamps},$timeInSeconds")
  pr.close()
  logger.debug(s"Finished in time $timeInSeconds")
  private val edgeFiles: Array[File] = resultDirEdges.listFiles()
  logger.debug(s"Finished compatibility graph creation, found ${edgeFiles.size} edge files")
  val endTime = System.currentTimeMillis()
  val dataset = new File(rolsetFile).getName.split("\\.")(0)
  println("dataset,nThreads,thresholdForFork,maxPairwiseListSizeForSingleThread,roleSamplingRate,timestampSamplingRate,runtimeInSeconds")
  println(s"$dataset,$nthreads,$thresholdForFork,$maxPairwiseListSizeForSingleThread,$roleSamplingRate,$timestampSamplingRate,$timeInSeconds")
}
