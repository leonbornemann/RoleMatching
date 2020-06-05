package de.hpi.dataset_versioning.data.diff.syntactic

import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService

object SnapshotRestoreMain extends App {
  val socrataDir = args(0)
  IOService.socrataDir = socrataDir
  IOService.printSummary()
  val version = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val transformer = new DiffManager(7)
  transformer.restoreFullSnapshotFromDiff(version,recursivelyRestoreSnapshots = true)
}
