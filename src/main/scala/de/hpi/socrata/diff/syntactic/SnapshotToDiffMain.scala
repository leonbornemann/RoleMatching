package de.hpi.socrata.diff.syntactic

import de.hpi.socrata.io.Socrata_IOService

import java.io.File

object SnapshotToDiffMain extends App {
  Socrata_IOService.socrataDir = args(0)
  Socrata_IOService.printSummary()
  val transformer = new DiffManager(7)
  val batchMode = args.size == 3 && args(2).toBoolean
  transformer.replaceAllNonCheckPointsWithDiffs(new File(args(1)),batchMode = batchMode)

}
