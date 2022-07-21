package de.hpi.role_matching.wikipedia_data_preparation.original_infobox_data

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.data.{ReservedChangeValues, RoleLineage}
import de.hpi.role_matching.wikipedia_data_preparation.original_infobox_data.InfoboxRevisionHistory.{EARLIEST_HISTORY_TIMESTAMP, LATEST_HISTORY_TIMESTAMP, TIME_AXIS, lowestGranularityInDays}
import de.hpi.role_matching.wikipedia_data_preparation.original_infobox_data.WikipediaLineageCreationMode.{WILDCARD_BETWEEN_ALL_CONFIRMATIONS, WILDCARD_BETWEEN_CHANGE, WILDCARD_OUTSIDE_OF_GRACE_PERIOD, WikipediaLineageCreationMode}
import de.hpi.role_matching.wikipedia_data_preparation.transformed.{TimeRangeToSingleValueReducer, WikipediaRoleLineage}

import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import java.util.regex.Pattern
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class InfoboxRevisionHistory(key:String,revisions:collection.Seq[InfoboxRevision]) {

  def integrityCheck() = {
    assert(revisionsSorted.size == revisionsSorted.map(_.revisionId).toSet.size)
    assert(revisionsSorted.size == revisionsSorted.map(_.validFrom).toSet.size)
  }

  val revisionsSorted = revisions
    .filter(r => r.validTo.isEmpty || r.validFrom!=r.validTo.get)
    .sortBy(r => r.validFromAsDate)
    .toIndexedSeq

  val wikilinkPattern = Pattern.compile("\\[\\[((?:\\w+:)?[^<>\\[\\]\"\\|]+)(?:\\|[^\\n\\]]+)?\\]\\]")

  val propToValueHistory:collection.mutable.HashMap[String,collection.mutable.TreeMap[LocalDateTime,String]] = collection.mutable.HashMap[String,collection.mutable.TreeMap[LocalDateTime,String]]()

  val valueConfirmationPoints = revisionsSorted
    .map(r => InfoboxRevisionHistory.TIME_AXIS.maxBefore(r.validFromAsDate.toLocalDate.plusDays(1)).get).toSet
  val earliestInsertOfThisInfobox = valueConfirmationPoints.min

  //just to get some stats:
  val propToMaxLinkCount = scala.collection.mutable.HashMap[String,Int]()


  def updateHistory(p: InfoboxProperty, newValue: String,t:LocalDateTime) = {
    val curHistory = propToValueHistory.getOrElseUpdate(p.name,collection.mutable.TreeMap[LocalDateTime,String]())
    if(curHistory.isEmpty && t!=EARLIEST_HISTORY_TIMESTAMP){
      if(earliestInsertOfThisInfobox.isAfter(EARLIEST_HISTORY_TIMESTAMP)) {
        curHistory.put(EARLIEST_HISTORY_TIMESTAMP.atStartOfDay(),ReservedChangeValues.NOT_EXISTANT_ROW)
      }
      if(t.toLocalDate.isAfter(earliestInsertOfThisInfobox)){
        curHistory.put(earliestInsertOfThisInfobox.atStartOfDay(),ReservedChangeValues.NOT_EXISTANT_CELL)
      }
    }
    if(newValue==ReservedChangeValues.NOT_EXISTANT_CELL && curHistory.last._2 == ReservedChangeValues.NOT_EXISTANT_CELL){
      //do nothing - this is an edge case that can happen for changes that are immediately reverted!
    } else {
      if(!curHistory.isEmpty)
        assert(newValue!=curHistory.last._2)
      curHistory.put(t,newValue)
    }
  }

  def integrityCheckHistories() = {
    propToValueHistory.foreach{case (k,vh) => {
      val vhAsIndexedSeq = vh.toIndexedSeq
      assert(vh.head._1.toLocalDate==EARLIEST_HISTORY_TIMESTAMP)
      for(i <- 1 until vhAsIndexedSeq.size){
        if(!(vhAsIndexedSeq(i-1)._2!=vhAsIndexedSeq(i)._2))
          println() //TODO: what warum taucht dieser Bug jetzt auf einmal auf? Späterer Time end kann nicht sein - werden daten modifiziert?
        assert(vhAsIndexedSeq(i-1)._2!=vhAsIndexedSeq(i)._2)
        assert(vhAsIndexedSeq(i-1)._1.isBefore(vhAsIndexedSeq(i)._1))
      }
    }}
  }

  def addValueToSequence(curSequence: ArrayBuffer[(LocalDate, String)], t: LocalDate, v: String,insertWildcardAfter:Boolean) = {
    if(curSequence.isEmpty || curSequence.last._2!=v){
      curSequence.append((t,v))
      val nextTimePoint = t.plusDays(lowestGranularityInDays)
      if(insertWildcardAfter && !valueConfirmationPoints.contains(nextTimePoint) && v!=ReservedChangeValues.DECAYED && !nextTimePoint.isAfter(InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP))
        curSequence.append((nextTimePoint,ReservedChangeValues.DECAYED))
    } else {
      //we confirm this value, but it is the same as the previous so no need to insert anything new, but we might need to insert wildcard on the next day
      val nextTimePoint = t.plusDays(lowestGranularityInDays)
      if(insertWildcardAfter && !valueConfirmationPoints.contains(nextTimePoint) && v!=ReservedChangeValues.DECAYED && !nextTimePoint.isAfter(InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP))
        curSequence.append((nextTimePoint,ReservedChangeValues.DECAYED))
    }
  }

  def applyNonProbabilisticDecay() = {
    val confirmationPointsSorted = valueConfirmationPoints.toIndexedSeq.sortBy(_.toEpochDay)
    val factLineages = propToValueHistory.map{case (k,valueHistory) => {
      val lineage = scala.collection.mutable.ArrayBuffer[(LocalDate,String)]()
      if(!valueConfirmationPoints.contains(EARLIEST_HISTORY_TIMESTAMP)){
        addValueToSequence(lineage,EARLIEST_HISTORY_TIMESTAMP,ReservedChangeValues.NOT_EXISTANT_ROW,false)
      }
      confirmationPointsSorted
        .zipWithIndex
        .foreach{case (ld,i) => {
          val value = new TimeRangeToSingleValueReducer(ld,ld.plusDays(lowestGranularityInDays),valueHistory,true).computeValue()
          if(i<confirmationPointsSorted.size-1){
            val nextDate = confirmationPointsSorted(i+1)
            if(mode==WikipediaLineageCreationMode.WILDCARD_BETWEEN_CHANGE){
              val nextValue = new TimeRangeToSingleValueReducer(nextDate,nextDate.plusDays(lowestGranularityInDays),valueHistory,true).computeValue()
              val insertWildcardAfter = nextValue!=value
              addValueToSequence(lineage,ld,value,insertWildcardAfter)
            } else if(mode==WikipediaLineageCreationMode.WILDCARD_BETWEEN_ALL_CONFIRMATIONS){
              val insertWildcardAfter = valueConfirmationPoints.contains(nextDate)
              addValueToSequence(lineage,ld,value,insertWildcardAfter)
            } else{
              assert(mode==WILDCARD_OUTSIDE_OF_GRACE_PERIOD)
              addValueToSequence(lineage,ld,value,false)
              val nextChangeIsAfterGracePeriod = nextDate.isAfter(ld.plusDays(getGracePeriodInDays))
              if(nextChangeIsAfterGracePeriod)
                addValueToSequence(lineage,ld.plusDays(getGracePeriodInDays),ReservedChangeValues.DECAYED,false)
            }
          } else {
            if(mode==WILDCARD_BETWEEN_CHANGE) {
              addValueToSequence(lineage,ld,value,false)
            } else if(mode==WILDCARD_BETWEEN_ALL_CONFIRMATIONS){
              val insertWildcardAfter = ld!=LATEST_HISTORY_TIMESTAMP
              addValueToSequence(lineage,ld,value,insertWildcardAfter)
            } else {
              assert(mode==WILDCARD_OUTSIDE_OF_GRACE_PERIOD)
              addValueToSequence(lineage,ld,value,false)
              val nextChangeIsAfterGracePeriod = LATEST_HISTORY_TIMESTAMP.isAfter(ld.plusDays(getGracePeriodInDays))
              if(nextChangeIsAfterGracePeriod)
                addValueToSequence(lineage,ld.plusDays(getGracePeriodInDays),ReservedChangeValues.DECAYED,false)
            }
          }

        }}
      lineage
        .zipWithIndex
        .foreach(t => {
          assert(t._2==0 || lineage(t._2-1)._2 != t._1._2 && lineage(t._2-1)._1.isBefore(t._1._1))
        })
      (k,RoleLineage(collection.mutable.TreeMap[LocalDate,Any]() ++ lineage))
    }}
    factLineages
  }

  def applyNoDecay() = {
    val roleLineages = propToValueHistory.map { case (k, valueHistory) => {
      val withOutVandalism = removeVandalism(valueHistory)
      integrityCheckRoleLineage(withOutVandalism)
      (k, RoleLineage(collection.mutable.TreeMap[LocalDate, Any]() ++ withOutVandalism))
    }}
    roleLineages
  }

  def transformGranularityAndExpandTimeRange = {
//    val relevantTimePoints = revisionsSorted
//      .map(r => r.validFromAsDate)
//      .toSet
//      .toIndexedSeq
//      .sorted
//    //map this to the ranges that are relevant:
//
    if(mode == WikipediaLineageCreationMode.PROBABILISTIC_DECAY_FUNCTION){
      applyProbabilisticDecay
    } else if(mode==WikipediaLineageCreationMode.NO_DECAY){
      applyNoDecay()
    } else{
      applyNonProbabilisticDecay()
    }

  }

  def probabilisticDecay(withOutVandalism: IndexedSeq[((LocalDate, String), Int)]) = {
    val durationsInTrainTime = withOutVandalism.tail.tail
      .withFilter{ case ((ld, _), i) => !ld.isAfter(trainTimeEnd)}
      .map { case ((ld, _), i) => ChronoUnit.DAYS.between(withOutVandalism(i - 1)._1._1, ld) }
      .sorted
    val indexOfCutoff = math.ceil(minDecayProbability * durationsInTrainTime.size).toInt -1
    if(!durationsInTrainTime.isEmpty){
      val decayTimeInDays = durationsInTrainTime(indexOfCutoff)
      assert((decayTimeInDays % lowestGranularityInDays) == 0)
      val lineage = withOutVandalism
        .flatMap { case ((ld, v), i) =>
          val endDate = if (i == withOutVandalism.size-1) LATEST_HISTORY_TIMESTAMP else withOutVandalism(i + 1)._1._1
          val duration = ChronoUnit.DAYS.between(ld, endDate)
          if (duration > decayTimeInDays && !RoleLineage.isWildcard(v))
            Seq((ld, v), (ld.plusDays(decayTimeInDays), ReservedChangeValues.DECAYED))
          else
            Seq((ld, v))
        }
      lineage
    } else {
      withOutVandalism.map(_._1)
    }
  }

  def removeDuplicates(lineageWithDuplicates: IndexedSeq[(LocalDate, String)]) = {
    val withIndex = lineageWithDuplicates.zipWithIndex
    withIndex
      .filter{ case ((ld, v), i) => i == 0 || v != withIndex(i - 1)._1._2 }
      .map(_._1)
  }

  private def applyProbabilisticDecay = {
    val roleLineages = propToValueHistory.flatMap { case (k, valueHistory) =>
      val withOutVandalism: IndexedSeq[((LocalDate, String), Int)] = removeVandalism(valueHistory)
        .zipWithIndex
      assert(withOutVandalism.forall { case ((ld, v), i) =>
        i == 0 || v != withOutVandalism(i - 1)._2
      })
      //we are skipping the last change because we can't compute a duration for it
      //we call tail twice so we skip the artificial duration in the beginning (we before inserted wildcard at the startpoint)
      if(withOutVandalism.size>1) {
        val lineage = removeDuplicates(probabilisticDecay(withOutVandalism))
        integrityCheckRoleLineage(lineage)
        Seq((k, RoleLineage(collection.mutable.TreeMap[LocalDate, Any]() ++ lineage)))
      } else {
        Seq()
      }
    }
    roleLineages
  }

  private def removeVandalism(valueHistory: mutable.TreeMap[LocalDateTime, String]) = {
    if (!valueHistory.contains(EARLIEST_HISTORY_TIMESTAMP.atStartOfDay())) {
      valueHistory.put(EARLIEST_HISTORY_TIMESTAMP.atStartOfDay(), ReservedChangeValues.NOT_EXISTANT_CELL)
    }
    val datesSorted = valueHistory.keySet.toIndexedSeq
    val withOutVandalismWithDuplicates = datesSorted
      .map { case (ldt) =>
        val beginInTimeaxis = TIME_AXIS.maxBefore(ldt.toLocalDate.plusDays(1)).get
        val value = new TimeRangeToSingleValueReducer(beginInTimeaxis, beginInTimeaxis.plusDays(lowestGranularityInDays), valueHistory, true).computeValue()
        (beginInTimeaxis, value)
      }
    //filter duplicates:
    val withOutVandalism = removeDuplicates(withOutVandalismWithDuplicates)
    withOutVandalism
  }

  private def integrityCheckRoleLineage(lineage: IndexedSeq[(LocalDate, String)]) = {
    lineage
      .zipWithIndex
      .foreach { case ((ld, v), i) =>
        assert(i == 0 || lineage(i - 1)._2 != v && lineage(i - 1)._1.isBefore(ld))
      }
    assert(lineage(0)._1 == EARLIEST_HISTORY_TIMESTAMP)
    //assert that all timestamps are multiples of granularity:
    assert(lineage.forall { case (ld, _) => ChronoUnit.DAYS.between(EARLIEST_HISTORY_TIMESTAMP, ld) % lowestGranularityInDays == 0 })
  }

  def updateHistoryIfChanged(property: InfoboxProperty, newValue: String, t: LocalDateTime) = {
    val curHistory = propToValueHistory.get(property.name)
    if(curHistory.isEmpty || curHistory.get.last._2!=newValue)
      updateHistory(property,newValue,t)
  }

  def countLinks(p: String, value: String) = {
    val matcher = wikilinkPattern.matcher(value)
    var count = 0
    while(matcher.find()){
      count +=1
    }
    val curMax = propToMaxLinkCount.getOrElse(p,-1)
    if(curMax<count)
      propToMaxLinkCount(p) = count
  }

  def extractExtraLinkHistories() = {
    val linkPositions = (0 until 10).toIndexedSeq
    propToValueHistory.toIndexedSeq.foreach{case (p,vh) => {
      vh.map{case (t,value) => {
        val matcher = wikilinkPattern.matcher(value)
        var curLinkPosition = 0
        while(matcher.find() && curLinkPosition < linkPositions.size){
          val linkTarget = matcher.group(1)
          updateHistoryIfChanged(new InfoboxProperty("extra_link",p + s"_🔗_extractedLink$curLinkPosition"),linkTarget,t)
          curLinkPosition+=1
        }
        //count what we could have found:
        countLinks(p,value)
      }}
    }}
  }

  def toWikipediaInfoboxValueHistories = {
    propToValueHistory.clear()
    revisionsSorted.foreach(r => {
      r.changes
        .withFilter(_.property.propertyType!="meta")
        .foreach(c => {
        val p = c.property
        val e = r.key
        val newValue = if(c.currentValue.isDefined) c.currentValue.get else ReservedChangeValues.NOT_EXISTANT_CELL
        updateHistoryIfChanged(p,newValue,r.validFromAsDate)
      })
    })
    //propToValueHistory("map2 cap")
    extractExtraLinkHistories()
    integrityCheckHistories()
    val lineages = transformGranularityAndExpandTimeRange
      .withFilter(_._2.lineage.values.exists(v => !RoleLineage.isWildcard(v)))
      .map(t => {
        WikipediaRoleLineage(revisions.head.template,revisions.head.pageID,revisions.head.key,t._1,t._2.toSerializationHelper)
      })
    lineages
  }

  def getGracePeriodInDays = {
    28
  }

  def mode = InfoboxRevisionHistory.lineageCreationMode.get
  def minDecayProbability = InfoboxRevisionHistory.minDecayProbability.get
  def trainTimeEnd = InfoboxRevisionHistory.trainTimeEnd.get

}
object InfoboxRevisionHistory extends StrictLogging{

  var lineageCreationMode:Option[WikipediaLineageCreationMode] = None
  var minDecayProbability:Option[Double] = None
  var trainTimeEnd:Option[LocalDate] = None

  def getFromRevisionCollection(objects:collection.Seq[InfoboxRevision]) = {
    objects
      .groupBy(_.key)
      .map(t => InfoboxRevisionHistory(t._1,t._2.toIndexedSeq))
  }

  private var lowestGranularityInDays = 1

  val LATEST_HISTORY_TIMESTAMP = LocalDate.parse("2019-09-07") //train end: 5 May 2011
  val EARLIEST_HISTORY_TIMESTAMP = LocalDate.parse("2003-01-04")
  var TIME_AXIS = recomputeTimeAxis

  private def recomputeTimeAxis = {
    val axis = collection.mutable.TreeSet[LocalDate]() ++ EARLIEST_HISTORY_TIMESTAMP.toEpochDay.to(LATEST_HISTORY_TIMESTAMP.toEpochDay).by(lowestGranularityInDays)
      .map(l => LocalDate.ofEpochDay(l))
    axis
  }

  def setGranularityInDays(g:Int) = {
    lowestGranularityInDays = g
    TIME_AXIS = recomputeTimeAxis
  }
}
