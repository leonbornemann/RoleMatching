package de.hpi.tfm.data.socrata

import de.hpi.tfm.io.IOService

object StatusReportMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
}
