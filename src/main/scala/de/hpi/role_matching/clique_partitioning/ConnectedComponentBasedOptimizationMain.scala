package de.hpi.role_matching.clique_partitioning


import de.hpi.role_matching.compatibility.graph.representation.slim.{SLimGraph, SlimGraphSet, SlimGraphWithoutWeight}

import java.io.File
import java.time.LocalDate

object ConnectedComponentBasedOptimizationMain extends App {
  val inputGraphFile = args(0)
  val trainTimeEnd = LocalDate.parse(args(1))
  val weightConfig = ScoreConfig.fromCLIArguments(args(2),args(3))
  val runGreedyOnly = args(4).toBoolean
  val graph = SlimGraphSet.fromJsonFile(inputGraphFile)
  val optimizationGraph = graph.transformToOptimizationGraph(trainTimeEnd,weightConfig)
  val resultDir = new File(args(5))
  val mdmcpExportDir = new File(args(6))
  val vertexLookupDirForPartitions = new File(args(7))
  val greedyMergeDir = new File(args(8))
  val optimizer = new HybridOptimizer(optimizationGraph, resultDir, mdmcpExportDir, vertexLookupDirForPartitions, greedyMergeDir,runGreedyOnly)
  optimizer.runComponentWiseOptimization()
  //optimizer.printComponentSizeHistogram()
}
