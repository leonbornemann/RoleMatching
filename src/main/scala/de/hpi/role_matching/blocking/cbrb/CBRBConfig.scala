package de.hpi.role_matching.blocking.cbrb

import java.time.LocalDate

case class CBRBConfig(timeRangeStart: LocalDate, timeRangeEnd: LocalDate) {

  assert(!timeRangeStart.isAfter(timeRangeEnd))

}
