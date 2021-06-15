package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.evaluation.data.{MDMCPInputGraph, SlimGraph}

import java.io.File

object MDMCPInputExportMain extends App {
  val MDMCPInputFile = args(0)
  val targetFile = new File(args(1))
  val graph = MDMCPInputGraph.fromJsonFile(MDMCPInputFile)
    .serializeToMDMCPInputFile(targetFile)
  //graph.toMDMCPGraph(scoringFunctionThreshold).serializeToMDMCPInputFile(MDMCPInputFile)

}
