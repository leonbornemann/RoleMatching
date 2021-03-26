package de.hpi.tfm.data.socrata.diff.syntactic

import de.hpi.tfm.io.IOService

import java.io.File

object SnapshotToDiffMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
  val transformer = new DiffManager(7)
  val batchMode = args.size == 3 && args(2).toBoolean
  transformer.replaceAllNonCheckPointsWithDiffs(new File(args(1)),batchMode = batchMode)

}
