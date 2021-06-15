package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.evaluation.data.{SLimGraph, SlimGraphOld}

import java.io.File

object MDMCPInputExportMain extends App {
  val MDMCPInputFile = args(0)
  val targetFile = new File(args(1))
  val graph = SLimGraph.fromJsonFile(MDMCPInputFile)
    .serializeToMDMCPInputFile(targetFile)

}
