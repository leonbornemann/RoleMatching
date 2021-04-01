package de.hpi.tfm.compatibility

import de.hpi.tfm.io.DBSynthesis_IOService.dateToStr

import java.time.LocalDate

case class GraphConfig(minEvidence: Int, timeRangeStart: LocalDate, timeRangeEnd: LocalDate) {

  def toFileNameString = {
    s"${minEvidence}_${dateToStr(timeRangeStart)}_${dateToStr(timeRangeEnd)}"
  }

}
