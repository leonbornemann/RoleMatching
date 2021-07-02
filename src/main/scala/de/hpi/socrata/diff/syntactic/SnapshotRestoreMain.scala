package de.hpi.socrata.diff.syntactic

import de.hpi.socrata.io.Socrata_IOService

import java.time.LocalDate

object SnapshotRestoreMain extends App {
  val socrataDir = args(0)
  Socrata_IOService.socrataDir = socrataDir
  Socrata_IOService.printSummary()
  val version = LocalDate.parse(args(1),Socrata_IOService.dateTimeFormatter)
  val transformer = new DiffManager(7)
  transformer.restoreFullSnapshotFromDiff(version,recursivelyRestoreSnapshots = true)
}
