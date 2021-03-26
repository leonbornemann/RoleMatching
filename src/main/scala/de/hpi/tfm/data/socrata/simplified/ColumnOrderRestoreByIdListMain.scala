package de.hpi.tfm.data.socrata.simplified

import de.hpi.tfm.io.IOService

import scala.io.Source

object ColumnOrderRestoreByIdListMain extends App {
  IOService.socrataDir = args(0)
  val ids = Source.fromFile(args(1))
    .getLines()
    .toSet
  val restorer = new ColumnOrderRestorer
  restorer.restoreForAllForIdList(ids)
}
