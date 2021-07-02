package de.hpi.socrata.simplified

import de.hpi.socrata.io.Socrata_IOService

import java.time.LocalDate

object MinimalHistoryCreationMain extends App {

  Socrata_IOService.socrataDir = args(0)
  val beginDate = LocalDate.parse(args(1))
  val endDate = LocalDate.parse(args(2))
  Socrata_IOService.extractMinimalHistoryInRange(beginDate,endDate)


}
