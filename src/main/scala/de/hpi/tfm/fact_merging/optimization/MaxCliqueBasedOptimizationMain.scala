package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.io.IOService

import java.io.File

object MaxCliqueBasedOptimizationMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val connectedComponentFile = args(2)
  //TODO: load connected components:
  val optimizer = new GreedyMaxCliqueBasedOptimizer(subdomain,new File(connectedComponentFile))
  optimizer.run()

}
