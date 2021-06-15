package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.evaluation.data.SlimGraph

import java.io.File

object MDMCPInputExportFromSlimGraphMain extends App {
  val slimGraphFile = args(0)
  val MDMCPInputFile = new File(args(1))
  val scoringFunctionThreshold = args(2).toDouble //0.460230 for politics for this score
  val graph = SlimGraph.fromJsonFile(slimGraphFile)
  graph.toMDMCPGraph(scoringFunctionThreshold).serializeToMDMCPInputFile(MDMCPInputFile)

}
