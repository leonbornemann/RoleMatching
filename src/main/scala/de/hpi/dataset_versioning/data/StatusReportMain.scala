package de.hpi.dataset_versioning.data

import de.hpi.dataset_versioning.io.IOService

object StatusReportMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
}
