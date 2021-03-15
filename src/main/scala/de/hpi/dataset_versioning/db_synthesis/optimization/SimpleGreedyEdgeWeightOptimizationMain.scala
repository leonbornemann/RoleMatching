package de.hpi.dataset_versioning.db_synthesis.optimization

import de.hpi.dataset_versioning.io.IOService

import java.io.{File, PrintWriter}

object SimpleGreedyEdgeWeightOptimizationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val connectedComponentFile = args(2)
  val optimizer = new GreedyEdgeWeightOptimizer(subdomain,new File(connectedComponentFile))
  optimizer.run()
}
