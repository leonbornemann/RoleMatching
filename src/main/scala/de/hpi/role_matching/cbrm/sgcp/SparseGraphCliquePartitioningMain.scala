package de.hpi.role_matching.cbrm.sgcp

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.MemoryEfficientCompatiblityGraphSet
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate

object SparseGraphCliquePartitioningMain extends App with StrictLogging{
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val datasource = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(datasource)
  val inputGraphFile = args(1)
  val trainTimeEnd = LocalDate.parse(args(2))
  val weightConfig = if(args(3)=="MAX_RECALL" || args(3) == "UNWEIGHTED") None else Some(ScoreConfig.fromJsonFile(args(3)))
  val maxRecallSetting = args(3)=="MAX_RECALL"
  val baselineNoWeightSetting = args(3) == "UNWEIGHTED"
  private val resultDirName = if(maxRecallSetting) "MAX_RECALL" else if (baselineNoWeightSetting) "UNWEIGHTED" else s"/alpha_${weightConfig.get.alpha}/"
  private val resultRootDir = args(4)
  val methodResultDir = new File(resultRootDir + resultDirName)
  val roleMergeResultDir = new File(methodResultDir + "/role_merges/")
  val weightConfigDir = new File(methodResultDir + s"/weightSettings/")
  val mdmcpExportDir = new File(methodResultDir + "/GeneratedMDMCPInputFiles/")
  val vertexLookupDirForPartitions = new File(methodResultDir + "/MDMCPVertexMapping/")
  val roleset = if(maxRecallSetting) Some(Roleset.fromJsonFile(args(5))) else None
  var graph = MemoryEfficientCompatiblityGraphSet.fromJsonFile(inputGraphFile)
  val optimizationGraph = {
    if(maxRecallSetting)
      graph.getMaxRecallSettingOptimizationGraph(trainTimeEnd,roleset.get)
    else if(baselineNoWeightSetting)
      graph.getBaselineNoWeightSetting(trainTimeEnd)
    else
      graph.transformToOptimizationGraph(trainTimeEnd,weightConfig.get)
  }
  graph = null //might help out the garbage collector
  Seq(roleMergeResultDir,mdmcpExportDir,weightConfigDir,vertexLookupDirForPartitions).foreach(_.mkdirs())
  logger.debug("Deleting old result files")
  Seq(roleMergeResultDir,mdmcpExportDir,vertexLookupDirForPartitions).foreach(d => {
    logger.debug(s"Deleting all files in ${d.getAbsolutePath}")
    d.listFiles().foreach(_.delete())
  })
  logger.debug("Finished Deleting old result files")
  val optimizer = new SGCPOptimizer(optimizationGraph, roleMergeResultDir, mdmcpExportDir,vertexLookupDirForPartitions)
  optimizer.runComponentWiseOptimization()
}
