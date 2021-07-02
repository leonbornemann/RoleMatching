package de.hpi.role_matching.compatibility

import de.hpi.socrata.io.Socrata_Synthesis_IOService.dateToStr

import java.time.LocalDate

case class GraphConfig(minEvidence: Int, timeRangeStart: LocalDate, timeRangeEnd: LocalDate) {

  assert(!timeRangeStart.isAfter(timeRangeEnd))

  def toFileNameString = {
    s"${minEvidence}_${dateToStr(timeRangeStart)}_${dateToStr(timeRangeEnd)}"
  }

}
