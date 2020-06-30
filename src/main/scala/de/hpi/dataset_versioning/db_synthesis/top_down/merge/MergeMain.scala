package de.hpi.dataset_versioning.db_synthesis.top_down.merge

import de.hpi.dataset_versioning.io.IOService

object MergeMain extends App {
  IOService.socrataDir = args(0)
  val merger = new TableMergeExecutor()
  merger.mergeTables()


}
