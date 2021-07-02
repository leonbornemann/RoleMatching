package de.hpi.role_matching.clique_partitioning


import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.compatibility.graph.representation.slim.{SLimGraph, SlimGraphSet, SlimGraphWithoutWeight}

import java.io.File
import java.time.LocalDate

object ConnectedComponentBasedOptimizationMain extends App with StrictLogging{
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val inputGraphFile = args(0)
  val trainTimeEnd = LocalDate.parse(args(1))
  val weightConfig = ScoreConfig.fromCLIArguments(args(2),args(3))
  val runGreedyOnly = args(4).toBoolean
  var graph = SlimGraphSet.fromJsonFile(inputGraphFile)
  val optimizationGraph = graph.transformToOptimizationGraph(trainTimeEnd,weightConfig)
  graph = null //might help out the garbage collector
  private val alphaDirName = s"/alpha_${args(3)}/"
  val resultDir = new File(args(5) + alphaDirName)
  val mdmcpExportDir = new File(args(6) + alphaDirName)
  val vertexLookupDirForPartitions = new File(args(7) + alphaDirName)
  val greedyMergeDir = new File(args(8) + alphaDirName)
  Seq(resultDir,mdmcpExportDir,vertexLookupDirForPartitions,greedyMergeDir).foreach(_.mkdirs())
  val optimizer = new HybridOptimizer(optimizationGraph, resultDir, mdmcpExportDir, vertexLookupDirForPartitions, greedyMergeDir,runGreedyOnly)
  optimizer.runComponentWiseOptimization()
  //optimizer.printComponentSizeHistogram()
}
