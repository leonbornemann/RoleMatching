package de.hpi.dataset_versioning.data.diff.syntactic

import de.hpi.dataset_versioning.io.IOService

object DiffCreationMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
  val transformer = new DiffManager(7)
  transformer.calculateAllDiffs()
}
