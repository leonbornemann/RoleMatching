package de.hpi.role_matching.clique_partitioning


import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.compatibility.graph.representation.slim.{SLimGraph, SlimGraphSet, SlimGraphWithoutWeight, VertexLookupMap}

import java.io.File
import java.time.LocalDate

object SparseGraphCliquePartitioningMain extends App with StrictLogging{
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val inputGraphFile = args(0)
  val trainTimeEnd = LocalDate.parse(args(1))
  val weightConfig = if(args(2)=="max_recall" || args(2) == "baselineNoWeight") None else Some(ScoreConfig.fromJsonFile(args(2)))
  val runGreedyOnly = args(3).toBoolean
  val maxRecallSetting = args(2)=="max_recall"
  val baselineNoWeightSetting = args(2) == "baselineNoWeight"
  private val resultDirName = if(maxRecallSetting) "max_recall" else if (baselineNoWeightSetting) "baselineNoWeight" else s"/alpha_${weightConfig.get.alpha}/"
  private val resultRootDir = args(4)
  val roleMergeResultDir = new File(resultRootDir + resultDirName)
  val weightConfigDir = new File(resultRootDir + s"/weightSettings/")
  val mdmcpExportDir = new File(args(5) + resultDirName)
  val vertexLookupDirForPartitions = new File(args(6) + resultDirName)
  val greedyMergeDir = new File(args(7) + resultDirName)
  val vertexLookupMap = if(maxRecallSetting) Some(VertexLookupMap.fromJsonFile(args(8))) else None
  var graph = SlimGraphSet.fromJsonFile(inputGraphFile)
  val optimizationGraph = {
    if(maxRecallSetting)
      graph.getMaxRecallSettingOptimizationGraph(trainTimeEnd,vertexLookupMap.get)
    else if(baselineNoWeightSetting)
      graph.getBaselineNoWeightSetting(trainTimeEnd,vertexLookupMap.get)
    else
      graph.transformToOptimizationGraph(trainTimeEnd,weightConfig.get)
  }
  graph = null //might help out the garbage collector
  Seq(roleMergeResultDir,mdmcpExportDir,greedyMergeDir,weightConfigDir,vertexLookupDirForPartitions).foreach(_.mkdirs())
  logger.debug("Deleting old result files")
  Seq(roleMergeResultDir,mdmcpExportDir,greedyMergeDir,vertexLookupDirForPartitions).foreach(d => {
    logger.debug(s"Deleting all files in ${d.getAbsolutePath}")
    d.listFiles().foreach(_.delete())
  })
  logger.debug("Finished Deleting old result files")
  //weightConfig.toJsonFile(new File(weightConfigDir.getAbsolutePath + s"/$resultDirName.json"))
  val optimizer = new SGCPOptimizer(optimizationGraph, roleMergeResultDir, mdmcpExportDir,vertexLookupDirForPartitions, greedyMergeDir,runGreedyOnly)
  optimizer.runComponentWiseOptimization()
  //optimizer.printComponentSizeHistogram()
}
