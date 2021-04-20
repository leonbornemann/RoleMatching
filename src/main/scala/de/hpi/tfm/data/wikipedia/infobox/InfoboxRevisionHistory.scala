package de.hpi.tfm.data.wikipedia.infobox

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.change.{Change, ReservedChangeValues}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.data.wikipedia.infobox.InfoboxRevision.logger
import de.hpi.tfm.data.wikipedia.infobox.InfoboxRevisionHistory.{EARLIEST_HISTORY_TIMESTAMP, LATEST_HISTORY_TIMESTAMP, lowestGranularityInDays}
import de.hpi.tfm.evaluation.Histogram
import de.hpi.tfm.io.IOService

import java.lang.AssertionError
import java.time.{Duration, LocalDate, LocalDateTime, Period}
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern
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

  val propToValueHistory = collection.mutable.HashMap[String,collection.mutable.TreeMap[LocalDateTime,String]]()

  val valueConfirmationPoints = revisionsSorted.map(_.validFromAsDate.toLocalDate).toSet
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
    assert(newValue!=curHistory.last._2)
    curHistory.put(t,newValue)
  }

  def integrityCheckHistories() = {
    propToValueHistory.foreach{case (k,vh) => {
      val vhAsIndexedSeq = vh.toIndexedSeq
      assert(vh.head._1==EARLIEST_HISTORY_TIMESTAMP.atStartOfDay())
      for(i <- 1 until vhAsIndexedSeq.size){
        assert(vhAsIndexedSeq(i-1)._2!=vhAsIndexedSeq(i)._2)
        assert(vhAsIndexedSeq(i-1)._1.isBefore(vhAsIndexedSeq(i)._1))
      }
    }}
  }

  def addValueToSequence(curSequence: ArrayBuffer[(LocalDate, String)], t: LocalDate, v: String) = {
    if(curSequence.isEmpty || curSequence.last._2!=v){
      curSequence.append((t,v))
      val nextDay = t.plusDays(1)
      if(!valueConfirmationPoints.contains(nextDay) && v!=ReservedChangeValues.NOT_EXISTANT_CELL)
        curSequence.append((nextDay,ReservedChangeValues.NOT_EXISTANT_CELL))
    }
  }

  def transformGranularityAndExpandTimeRange() = {
//    val relevantTimePoints = revisionsSorted
//      .map(r => r.validFromAsDate)
//      .toSet
//      .toIndexedSeq
//      .sorted
//    //map this to the ranges that are relevant:
//
    val confirmationPointsSorted = valueConfirmationPoints.toIndexedSeq.sortBy(_.toEpochDay)
    val factLineages = propToValueHistory.map{case (k,valueHistory) => {
      val lineage = scala.collection.mutable.ArrayBuffer[(LocalDate,String)]()
      if(!valueConfirmationPoints.contains(EARLIEST_HISTORY_TIMESTAMP)){
        addValueToSequence(lineage,EARLIEST_HISTORY_TIMESTAMP,ReservedChangeValues.NOT_EXISTANT_ROW)
      }
      confirmationPointsSorted.foreach(ld => {
        val value = new TimeRangeToSingleValueReducer(ld,ld.plusDays(1),valueHistory,true).computeValue()
        addValueToSequence(lineage,ld,value)
      })

//      var curStart = EARLIEST_HISTORY_TIMESTAMP
//      var curEnd = curStart.plusDays(lowestGranularityInDays)
//      val latest = LATEST_HISTORY_TIMESTAMP
//
//      while(curEnd!=curStart){
//        val oldValueGetsConfirmed = (curStart.toEpochDay until curEnd.toEpochDay)
//          .map(l => LocalDate.ofEpochDay(l))
//          .exists(ld => valueConfirmationPoints.contains(ld))
//        val value = new TimeRangeToSingleValueReducer(curStart,curEnd,valueHistory,earliestInsertOfThisInfobox,oldValueGetsConfirmed).computeValue()
//        curSequence.append((curStart,value))
//        curStart = curEnd
//        curEnd = Seq(curStart.plusDays(lowestGranularityInDays),latest).min
//      }
//      assert(curSequence.size == LATEST_HISTORY_TIMESTAMP.toEpochDay - EARLIEST_HISTORY_TIMESTAMP.toEpochDay) //only works for daily!
//      //eliminate duplicates:
//      val lineage = (0 until curSequence.size)
//        .filter(i => i==0 || curSequence(i)._2 != curSequence(i-1)._2)
//        .map(i => curSequence(i))
      lineage
        .zipWithIndex
        .foreach(t => {
          if(!(t._2==0 || lineage(t._2-1)._2 != t._1._2 && lineage(t._2-1)._1.isBefore(t._1._1)))
            println()
          assert(t._2==0 || lineage(t._2-1)._2 != t._1._2 && lineage(t._2-1)._1.isBefore(t._1._1))
        })
      (k,FactLineage(collection.mutable.TreeMap[LocalDate,Any]() ++ lineage))
    }}
    factLineages
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
          updateHistoryIfChanged(new InfoboxProperty("extra_link",p + s"_link$curLinkPosition"),linkTarget,t)
          curLinkPosition+=1
        }
        //count what we could have found:
        countLinks(p,value)
      }}
    }}
  }

  def evalLinkCount() = {
    Histogram(propToMaxLinkCount.values.toIndexedSeq)
      .printAll()
  }

  def toPaddedInfoboxHistory = {
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
      .map(t => (t._1,t._2.toSerializationHelper))
    PaddedInfoboxHistory(revisions.head.template,revisions.head.pageID,revisions.head.pageTitle,revisions.head.key,lineages)
  }
}
object InfoboxRevisionHistory extends StrictLogging{

  def getFromRevisionCollection(objects:collection.Seq[InfoboxRevision]) = {
    objects
      .groupBy(_.key)
      .map(t => InfoboxRevisionHistory(t._1,t._2.toIndexedSeq))
  }

  val lowestGranularityInDays = 1

  if(lowestGranularityInDays!=1){
    throw new AssertionError("Implementation of transformGranularityAndExpandTimeRange only works with daily granularity - this should be changed to use ranges of days if the granularity is more course")
  }

  def LATEST_HISTORY_TIMESTAMP = LocalDate.parse("2019-09-02")
  def EARLIEST_HISTORY_TIMESTAMP = LocalDate.parse("2003-01-04")
}
