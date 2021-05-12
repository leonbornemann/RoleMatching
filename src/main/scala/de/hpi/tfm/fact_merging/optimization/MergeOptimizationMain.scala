package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate

object MergeOptimizationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val connectedComponentFile = new File(args(2))
  val optimizationMethodName = args(3)
  val targetFunctionName = args(4)
  val minEvidence = args(5).toInt
  val timeRangeStart = LocalDate.parse(args(6))
  val timeRangeEnd = LocalDate.parse(args(7))
  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
  GLOBAL_CONFIG.OPTIMIZATION_TARGET_FUNCTION_NAME = targetFunctionName
  val optimizer = GLOBAL_CONFIG.getOptimizer(optimizationMethodName,subdomain,connectedComponentFile,graphConfig)
  optimizer.run()
}
