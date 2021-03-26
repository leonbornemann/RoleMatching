package de.hpi.tfm.data.socrata.diff.syntactic

import de.hpi.tfm.io.IOService

object DiffCreationMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
  val transformer = new DiffManager(7)
  transformer.calculateAllDiffs()
}
