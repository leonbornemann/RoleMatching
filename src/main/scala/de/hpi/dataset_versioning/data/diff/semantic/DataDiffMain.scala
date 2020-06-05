package de.hpi.dataset_versioning.data.diff.semantic

import de.hpi.dataset_versioning.io.IOService

object DataDiffMain extends App {

  IOService.socrataDir = args(0)
  val diffCalculator = new DataDiffCalculator()
  //diffCalculator.calculateDataDiff(versions(0),versions(1))
  diffCalculator.aggregateAllDataDiffs()
}
