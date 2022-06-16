package de.hpi.role_matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.compatibility_graph.CompatibilityGraphCreationConfig
import de.hpi.role_matching.cbrm.data.UpdateChangeCounter
import de.hpi.wikipedia_data_preparation.original_infobox_data.InfoboxRevisionHistory

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object GLOBAL_CONFIG extends StrictLogging{

  var INDEXING_CONFIG = CompatibilityGraphCreationConfig(0.9,0.9,50)

  var INDEXING_STATS_RESULT_DIR: File = new File("stats/")

  def getEvaluationStepDurationInDays(dataSource: String): Int = if(dataSource=="wikipedia") 364 else 30

  var nonInformativeValues: Set[Any] = Set("", null)

  var granularityInDays = Int.MaxValue
  var trainTimeEnd = LocalDate.MIN
  var finalWikipediaTrainTimeENd = "2016-05-07"

  var OPTIMIZATION_TARGET_FUNCTION_NAME: String = ""
  var ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS = false

  //val CHANGE_COUNT_METHOD = new DatasetInsertIgnoreFieldChangeCounter()
  val CHANGE_COUNT_METHOD = new UpdateChangeCounter()
  var STANDARD_TIME_FRAME_START = LocalDate.parse("2019-11-01")
  var STANDARD_TIME_FRAME_END = LocalDate.parse("2020-04-30")
  val dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  def STANDARD_TIME_RANGE = (STANDARD_TIME_FRAME_START.toEpochDay to STANDARD_TIME_FRAME_END.toEpochDay by granularityInDays).map(LocalDate.ofEpochDay(_))

  def STANDARD_TIME_RANGE_SIZE = (STANDARD_TIME_FRAME_START.toEpochDay to STANDARD_TIME_FRAME_END.toEpochDay).size

  def setSettingsForDataSource(dataSource: String,timeFactor:Double = 1.0) = {
    if(dataSource=="wikipedia"){
      STANDARD_TIME_FRAME_START=LocalDate.parse("2003-01-04")
      STANDARD_TIME_FRAME_END=LocalDate.parse("2019-09-07")
      InfoboxRevisionHistory.setGranularityInDays(7)
      granularityInDays=7
    } else if(dataSource=="socrata"){
      STANDARD_TIME_FRAME_START=LocalDate.parse("2019-11-01")
      STANDARD_TIME_FRAME_END=LocalDate.parse("2020-11-01")
      InfoboxRevisionHistory.setGranularityInDays(1)
      granularityInDays=1
    } else {
      logger.debug("Unknown data source specified!")
      assert(false)
    }
    if(timeFactor!=1.0){
      STANDARD_TIME_FRAME_END = STANDARD_TIME_FRAME_START
        .plusDays((ChronoUnit.DAYS.between(STANDARD_TIME_FRAME_START,STANDARD_TIME_FRAME_END)*timeFactor).toInt)
    }

  }

  def dateToStr(date: LocalDate) = GLOBAL_CONFIG.dateTimeFormatter.format(date)

  var random = new scala.util.Random(12)
}
