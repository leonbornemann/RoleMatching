package de.hpi.dataset_versioning.db_synthesis.optimization

import de.hpi.dataset_versioning.io.IOService
import org.glassfish.jersey.server.model.Parameter.Source

import java.io.File

object MaxCliqueBasedOptimizationMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val connectedComponentFile = args(2)
  //TODO: load connected components:
  val optimizer = new GreedyMaxCliqueBasedOptimizer(subdomain,new File(connectedComponentFile))
  optimizer.run()

}
