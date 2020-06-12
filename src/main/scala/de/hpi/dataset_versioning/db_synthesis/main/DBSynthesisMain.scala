package de.hpi.dataset_versioning.db_synthesis.main

import de.hpi.dataset_versioning.io.IOService

object DBSynthesisMain extends App {

  //first step: load changes
  val id = "test-test"
  IOService.getAllSimplifiedDataVersions(id)


}
