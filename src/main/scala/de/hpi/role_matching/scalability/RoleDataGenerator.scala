package de.hpi.role_matching.scalability

import de.hpi.role_matching.data.{ReservedChangeValues, RoleLineage, RoleLineageWithHashMap, RoleLineageWithID, ValueDistribution}
import de.hpi.util.GLOBAL_CONFIG

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}
import scala.util.Random

class RoleDataGenerator(distinctValueCountDistribution: ValueDistribution[Int],
                        densityDistribution: ValueDistribution[Double],
                        valueInLineageDistribution: ValueDistribution[Any],
                        random:Random) {

  val standardtimerange = GLOBAL_CONFIG.STANDARD_TIME_RANGE
  val standardtimerangeWithoutLast = GLOBAL_CONFIG.STANDARD_TIME_RANGE.slice(0,standardtimerange.size-1)
  val treeSetTimeRange = collection.mutable.TreeSet[LocalDate]() ++ standardtimerangeWithoutLast
  val totalDaysInTimePeriod = ChronoUnit.DAYS.between(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, GLOBAL_CONFIG.STANDARD_TIME_FRAME_END).toDouble

  def timeShare(begin: LocalDate, end: LocalDate) = {
    ChronoUnit.DAYS.between(begin, end) / totalDaysInTimePeriod
  }

  def getClosestLegalDate(date: LocalDate) = {
    if(treeSetTimeRange.contains(date))
      date
    else {
      val before = treeSetTimeRange.maxBefore(date)
      val after = treeSetTimeRange.minAfter(date)
      if(before.isEmpty)
        after.get
      else if (after.isEmpty)
        before.get
      else {
        Seq(before,after).minBy(d => ChronoUnit.DAYS.between(d.get,date).abs).get
      }
    }
  }

  def getNextRoleLineage(): RoleLineageWithHashMap = {
    val distinctValueCount = distinctValueCountDistribution.drawWeightedRandomValue(random)
    val density = densityDistribution.drawWeightedRandomValue(random)
    val values = random.shuffle(valueInLineageDistribution.drawWeightedRandomValues(distinctValueCount,random)
      .toIndexedSeq)
    val entries = collection.mutable.TreeMap[LocalDate,Any]() ++ random
      .shuffle(standardtimerangeWithoutLast)
      .take(distinctValueCount)
      .zip(values)
    val targetShareOfWildcardTIme = 1.0 - density
    var curWildcardTime = timeShare(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, entries.firstKey)
    if(targetShareOfWildcardTIme < curWildcardTime){
      //we only need to correct the first timestamp
      val newTS = standardtimerange((targetShareOfWildcardTIme*standardtimerangeWithoutLast.size).toInt)
      assert(newTS.isBefore(entries.firstKey))
      val value = entries.remove(entries.firstKey).get
      entries.put(newTS,value)
      RoleLineage(entries).toSerializationHelper
    } else {
      val rlWithoutWildcard = RoleLineage(entries)
      rlWithoutWildcard
      //reduce density
      //method 1. get non wildcard time periods 2. randomly distribute wildcard times 3. randomly insert them into the time periods
      val beginTimesWithIndex = entries
        .toIndexedSeq
        .map(_._1)
        .zipWithIndex
      val nonWildcardTimePeriods = beginTimesWithIndex
        .map{case (ld,i) =>
          if(i==beginTimesWithIndex.size-1)
            (ld,GLOBAL_CONFIG.STANDARD_TIME_FRAME_END)
          else
            (ld,beginTimesWithIndex(i+1)._1)
        }
        .map(t => (t,timeShare(t._1.plusDays(GLOBAL_CONFIG.granularityInDays),t._2)))
      val it = nonWildcardTimePeriods
        .sortBy(_._2)
        .filter(t => t._1._1 != GLOBAL_CONFIG.STANDARD_TIME_FRAME_END.minusDays(GLOBAL_CONFIG.granularityInDays))
        .iterator
      val remainingWildcardTime = targetShareOfWildcardTIme - curWildcardTime
      for(((curStart,curEnd),maxDuration) <- it) {
        val durationThisInterval = ChronoUnit.DAYS.between(curStart,curEnd)
        //roll a duration
        val wildcardDurationForThisRange = maxDuration*remainingWildcardTime
        //upper border
        //lowerborder:
        val lower = curStart.plusDays(GLOBAL_CONFIG.granularityInDays)
        val upper = getClosestLegalDate(curEnd.minusDays((totalDaysInTimePeriod*wildcardDurationForThisRange).toLong))
        if(lower.isBefore(upper)) {
          val between = ChronoUnit.DAYS.between(lower,upper)
          var chosenBegin = lower.plusDays(random.nextLong(between))
          chosenBegin = getClosestLegalDate(chosenBegin)
          if(chosenBegin == curStart)
            chosenBegin = curStart.plusDays(GLOBAL_CONFIG.granularityInDays)
          var end = getClosestLegalDate(chosenBegin.plusDays((wildcardDurationForThisRange*durationThisInterval).toLong))
          assert(!end.isAfter(curEnd))
          assert(chosenBegin.isAfter(curStart))
          if(chosenBegin==curEnd){
            //skip
          } else {
            //add to lineage
            entries.put(chosenBegin,ReservedChangeValues.DECAYED)
            if(end != curEnd)
              entries.put(end,entries(curStart))
          }
        } else {
          //skip
        }

      }
      RoleLineage(entries).toSerializationHelper
    }
  }

  def generate(n:Int) = {
    (0 until n).map(i => {
      val id = s"$i"
      RoleLineageWithID(id,getNextRoleLineage())
    })
  }


}
