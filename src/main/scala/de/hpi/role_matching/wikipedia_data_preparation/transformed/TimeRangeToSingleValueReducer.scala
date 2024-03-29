package de.hpi.role_matching.wikipedia_data_preparation.transformed

import de.hpi.role_matching.data.ReservedChangeValues
import de.hpi.role_matching.wikipedia_data_preparation.original_infobox_data.InfoboxRevisionHistory

import java.time.{Duration, LocalDate, LocalDateTime, ZoneOffset}
import scala.collection.mutable

class TimeRangeToSingleValueReducer(curStart: LocalDate,
                                    curEnd: LocalDate,
                                    completeLineage: mutable.TreeMap[LocalDateTime, String],
                                    valueConfirmed:Boolean) {

  val valueToDuration = mutable.HashMap[String,Duration]()

  def getTimePeriod(start: LocalDateTime, end: LocalDateTime) = {
    Duration.between(start,end)
  }

  def computeValue():String = {
    val inRange = completeLineage.range(curStart.atStartOfDay(),curEnd.atStartOfDay()).toIndexedSeq
    if(inRange.isEmpty){
      if(valueConfirmed) {
        val prevValue = completeLineage.maxBefore(curStart.atStartOfDay())
        if(prevValue.isDefined) {
//          if(!(prevValue.get._1.toLocalDate==curStart.minusDays(InfoboxRevisionHistory.lowestGranularityInDays)))
//            println()
          //assert(prevValue.get._1.toLocalDate==curStart.minusDays(InfoboxRevisionHistory.lowestGranularityInDays))
          prevValue.get._2
        } else {
          assert(curStart == InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP)
          ReservedChangeValues.NOT_EXISTANT_CELL
        }
      } else if(curStart==InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP){
        ReservedChangeValues.NOT_EXISTANT_ROW
      } else {
        ReservedChangeValues.NOT_KNOWN_DUE_TO_NO_VISIBLE_CHANGE
      }
    } else{
      getMajorityValue(inRange)
    }
  }

  private def getMajorityValue(inRange: IndexedSeq[(LocalDateTime, String)]) = {
    val prevValueOption = completeLineage.maxBefore(curStart.atStartOfDay())
    if(prevValueOption.isDefined){
      val prevValue = prevValueOption.get._2
      val duration = getTimePeriod(curStart.atStartOfDay(), inRange(0)._1)
      valueToDuration.put(prevValue, duration)
    }
    inRange.zipWithIndex.foreach { case ((d, v), i) => {
      val prevDuration = valueToDuration.getOrElse(v, Duration.ZERO)
      val curDuration = if (i < inRange.size - 1)
        Duration.between(d, inRange(i + 1)._1)
      else
        Duration.between(d, curEnd.atStartOfDay())
      valueToDuration(v) = prevDuration.plus(curDuration)
    }
    }
    val totalDuration= valueToDuration.values.reduce(_.plus(_))
    val maxDuration = Duration.between(curStart.atStartOfDay().toInstant(ZoneOffset.UTC),curEnd.atStartOfDay().toInstant(ZoneOffset.UTC))
    assert(prevValueOption.isEmpty || valueToDuration.values.reduce(_.plus(_)) == Duration.between(curStart.atStartOfDay().toInstant(ZoneOffset.UTC),curEnd.atStartOfDay().toInstant(ZoneOffset.UTC)))
    valueToDuration.maxBy(_._2)._1
  }
}
