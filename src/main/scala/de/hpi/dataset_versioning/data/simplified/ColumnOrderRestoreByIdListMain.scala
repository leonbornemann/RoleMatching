package de.hpi.dataset_versioning.data.simplified

import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

object ColumnOrderRestoreByIdListMain extends App {
  IOService.socrataDir = args(0)
  val ids = Source.fromFile(args(1))
    .getLines()
    .toSet
  val restorer = new ColumnOrderRestorer
  restorer.restoreForAllForIdList(ids)
}
