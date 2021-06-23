package de.hpi.tfm.evaluation.data

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.fact_merging.optimization.{BruteForceComponentOptimizer, HybridOptimizer, SubGraph}
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.io.File

object ConnectedComponentBasedOptimizationMain extends App {
  val slimGraphFile = args(0)
  val resultFile = new File(args(1))
  val mdmcpExportDir = new File(args(2))
  val vertexLookupDirForPartitions = new File(args(3))
  val greedyMergeDir = new File(args(4)) //TODO: this is temporary for now - can be removed later!
  val graph = SLimGraph.fromJsonFile(slimGraphFile)
  val optimizer = new HybridOptimizer(graph.transformToOptimizationGraph,resultFile,mdmcpExportDir,vertexLookupDirForPartitions,greedyMergeDir)
  optimizer.runComponentWiseOptimization()
  //optimizer.printComponentSizeHistogram()
}
