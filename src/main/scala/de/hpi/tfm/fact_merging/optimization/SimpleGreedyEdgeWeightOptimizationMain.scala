package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.internal.InternalFactMatchGraphCreationMain.args
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate

object SimpleGreedyEdgeWeightOptimizationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val connectedComponentFile = args(2)
  val minEvidence = args(3).toInt
  val timeRangeStart = LocalDate.parse(args(4))
  val timeRangeEnd = LocalDate.parse(args(5))
  val targetFunctionName = args(6)
  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
  GLOBAL_CONFIG.OPTIMIZATION_TARGET_FUNCTION_NAME = targetFunctionName
  val optimizer = new GreedyEdgeWeightOptimizer(subdomain,new File(connectedComponentFile),graphConfig)
  optimizer.run()
}
