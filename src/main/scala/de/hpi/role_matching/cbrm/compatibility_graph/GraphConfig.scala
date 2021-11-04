package de.hpi.role_matching.cbrm.compatibility_graph

import de.hpi.role_matching.GLOBAL_CONFIG.dateToStr

import java.time.LocalDate

case class GraphConfig(minEvidence: Int, timeRangeStart: LocalDate, timeRangeEnd: LocalDate) {

  assert(!timeRangeStart.isAfter(timeRangeEnd))

  def toFileNameString = {
    s"${minEvidence}_${dateToStr(timeRangeStart)}_${dateToStr(timeRangeEnd)}"
  }

}
