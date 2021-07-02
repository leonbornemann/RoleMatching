package de.hpi.socrata

import de.hpi.socrata.io.Socrata_IOService

object StatusReportMain extends App {
  Socrata_IOService.socrataDir = args(0)
  Socrata_IOService.printSummary()
}
