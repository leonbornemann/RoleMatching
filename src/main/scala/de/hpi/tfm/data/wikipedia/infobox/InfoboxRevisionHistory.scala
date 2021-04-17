package de.hpi.tfm.data.wikipedia.infobox

import de.hpi.tfm.data.socrata.change.{Change, ReservedChangeValues}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.data.wikipedia.infobox.InfoboxRevisionHistory.{EARLIEST_HISTORY_TIMESTAMP, LATEST_HISTORY_TIMESTAMP, lowestGranularityInDays}
import de.hpi.tfm.io.IOService

import java.time.{Duration, LocalDate, LocalDateTime, Period}
import java.time.format.DateTimeFormatter

case class InfoboxRevisionHistory(key:String,revisions:collection.Seq[InfoboxRevision]) {

  val revisionsSorted = revisions.sortBy(r => r.validFromAsDate)
    .toIndexedSeq

  val propToValueHistory = collection.mutable.HashMap[String,collection.mutable.TreeMap[LocalDateTime,String]]()

  val valueConfirmationPoints = revisionsSorted.map(_.validFromAsDate.toLocalDate).toSet
  val earliestInsertOfThisInfobox = valueConfirmationPoints.min

  def updateHistory(p: String, newValue: String,t:LocalDateTime) = {
    val curHistory = propToValueHistory.getOrElseUpdate(p,collection.mutable.TreeMap[LocalDateTime,String]())
    if(curHistory.isEmpty && t!=EARLIEST_HISTORY_TIMESTAMP){
      if(earliestInsertOfThisInfobox.isAfter(EARLIEST_HISTORY_TIMESTAMP.toLocalDate)) {
        curHistory.put(EARLIEST_HISTORY_TIMESTAMP,ReservedChangeValues.NOT_EXISTANT_ROW)
      }
      if(t.toLocalDate.isAfter(earliestInsertOfThisInfobox)){
        curHistory.put(earliestInsertOfThisInfobox.atStartOfDay(),ReservedChangeValues.NOT_EXISTANT_CELL)
      }
    }
    if(!curHistory.isEmpty)
      assert(newValue!=curHistory.last._2)
    curHistory.put(t,newValue)
  }

  def integrityCheckHistories() = {
    propToValueHistory.values.foreach(vh => {
      val vhAsIndexedSeq = vh.toIndexedSeq
      assert(vh.head._1==EARLIEST_HISTORY_TIMESTAMP)
      for(i <- 1 until vhAsIndexedSeq.size){
        assert(vhAsIndexedSeq(i-1)._2!=vhAsIndexedSeq(i)._2)
        assert(vhAsIndexedSeq(i-1)._1.isBefore(vhAsIndexedSeq(i)._1))
      }
    })
  }

  def transformGranularityAndExpandTimeRange() = {
    val factLineages = propToValueHistory.map{case (k,valueHistory) => {
      var curStart = EARLIEST_HISTORY_TIMESTAMP.toLocalDate
      var curEnd = curStart.plusDays(lowestGranularityInDays)
      val latest = LATEST_HISTORY_TIMESTAMP.toLocalDate
      val curSequence = scala.collection.mutable.ArrayBuffer[(LocalDate,String)]()
      while(curEnd!=curStart){
        val oldValueGetsConfirmed = (curStart.toEpochDay until curEnd.toEpochDay)
          .map(l => LocalDate.ofEpochDay(l))
          .exists(ld => valueConfirmationPoints.contains(ld))
        val value = new TimeRangeToSingleValueReducer(curStart,curEnd,valueHistory,earliestInsertOfThisInfobox,oldValueGetsConfirmed).computeValue()
        curSequence.append((curStart,value))
        curStart = curEnd
        curEnd = Seq(curStart.plusDays(lowestGranularityInDays),latest).min
      }
      assert(curSequence.size == LATEST_HISTORY_TIMESTAMP.toLocalDate.toEpochDay - EARLIEST_HISTORY_TIMESTAMP.toLocalDate.toEpochDay) //only works for daily!
      //eliminate duplicates:
      val lineage = (0 until curSequence.size)
        .filter(i => i==0 || curSequence(i)._2 != curSequence(i-1)._2)
        .map(i => curSequence(i))
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

  def toPaddedInfoboxHistory = {
   revisionsSorted.foreach(r => {
      r.changes.foreach(c => {
        val p = c.property
        val e = r.key
        val newValue = if(c.currentValue.isDefined) c.currentValue.get else ReservedChangeValues.NOT_EXISTANT_CELL
        updateHistory(p,newValue,r.validFromAsDate)
      })
    })
    integrityCheckHistories()
    val lineages = transformGranularityAndExpandTimeRange
      .map(t => (t._1,t._2.toSerializationHelper))
    PaddedInfoboxHistory(revisions.head.template,revisions.head.pageID,revisions.head.pageTitle,revisions.head.key,lineages)
  }
}
object InfoboxRevisionHistory{
  val lowestGranularityInDays = 1

  def LATEST_HISTORY_TIMESTAMP = LocalDateTime.parse("2017-11-03T15:20:41") //TODO: find out the date of that LocalDate.parse("2016-11-01", IOService.dateTimeFormatter)
  def EARLIEST_HISTORY_TIMESTAMP = LocalDateTime.parse("2005-07-29T22:31:59") //TODO: find out the date of that LocalDate.parse("2016-11-01", IOService.dateTimeFormatter)
}
