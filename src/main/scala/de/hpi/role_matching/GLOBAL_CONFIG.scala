package de.hpi.role_matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.change.UpdateChangeCounter
import de.hpi.socrata.io.Socrata_IOService

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object GLOBAL_CONFIG extends StrictLogging{

  var nonInformativeValues: Set[Any] = Set("", null)

  var granularityInDays = Int.MaxValue
  var trainTimeEnd = LocalDate.MIN

  var OPTIMIZATION_TARGET_FUNCTION_NAME: String = ""
  var ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS = false

  //val CHANGE_COUNT_METHOD = new DatasetInsertIgnoreFieldChangeCounter()
  val CHANGE_COUNT_METHOD = new UpdateChangeCounter()
  var STANDARD_TIME_FRAME_START = LocalDate.parse("2019-11-01", Socrata_IOService.dateTimeFormatter)
  var STANDARD_TIME_FRAME_END = LocalDate.parse("2020-04-30", Socrata_IOService.dateTimeFormatter)
  val dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  def STANDARD_TIME_RANGE = (STANDARD_TIME_FRAME_START.toEpochDay to STANDARD_TIME_FRAME_END.toEpochDay).map(LocalDate.ofEpochDay(_))

  def STANDARD_TIME_RANGE_SIZE = (STANDARD_TIME_FRAME_START.toEpochDay to STANDARD_TIME_FRAME_END.toEpochDay).size

  def setDatesForDataSource(dataSource: String) = {
    if(dataSource=="wikipedia"){
      STANDARD_TIME_FRAME_START=LocalDate.parse("2003-01-04")
      STANDARD_TIME_FRAME_END=LocalDate.parse("2019-09-07")
    } else if(dataSource=="socrata"){
      STANDARD_TIME_FRAME_START=LocalDate.parse("2019-11-01")
      STANDARD_TIME_FRAME_END=LocalDate.parse("2020-11-01")
    } else {
      logger.debug("Unknown data source specified!")
      assert(false)
    }
  }
}
