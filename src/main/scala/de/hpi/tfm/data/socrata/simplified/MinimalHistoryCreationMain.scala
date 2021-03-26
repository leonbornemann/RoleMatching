package de.hpi.tfm.data.socrata.simplified

import de.hpi.tfm.io.IOService

import java.time.LocalDate

object MinimalHistoryCreationMain extends App {

  IOService.socrataDir = args(0)
  val beginDate = LocalDate.parse(args(1))
  val endDate = LocalDate.parse(args(2))
  IOService.extractMinimalHistoryInRange(beginDate,endDate)


}
