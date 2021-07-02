package de.hpi.wikipedia.data.original

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.change.ReservedChangeValues
import de.hpi.socrata.tfmp_input.table.nonSketch.FactLineage
import de.hpi.role_matching.clique_partitioning.Histogram
import de.hpi.wikipedia.data.original.InfoboxRevisionHistory.{EARLIEST_HISTORY_TIMESTAMP, LATEST_HISTORY_TIMESTAMP, lowestGranularityInDays}
import de.hpi.wikipedia.data.original.WikipediaLineageCreationMode.{WILDCARD_BETWEEN_ALL_CONFIRMATIONS, WILDCARD_BETWEEN_CHANGE, WILDCARD_OUTSIDE_OF_GRACE_PERIOD, WikipediaLineageCreationMode}
import de.hpi.wikipedia.data.transformed.{TimeRangeToSingleValueReducer, WikipediaInfoboxValueHistory}

import java.time.{LocalDate, LocalDateTime}
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
        assert(vhAsIndexedSeq(i-1)._2!=vhAsIndexedSeq(i)._2)
        assert(vhAsIndexedSeq(i-1)._1.isBefore(vhAsIndexedSeq(i)._1))
      }
    }}
  }

  def addValueToSequence(curSequence: ArrayBuffer[(LocalDate, String)], t: LocalDate, v: String,insertWildcardAfter:Boolean) = {
    if(curSequence.isEmpty || curSequence.last._2!=v){
      curSequence.append((t,v))
      val nextTimePoint = t.plusDays(lowestGranularityInDays)
      if(insertWildcardAfter && !valueConfirmationPoints.contains(nextTimePoint) && v!=ReservedChangeValues.NOT_EXISTANT_CELL && !nextTimePoint.isAfter(InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP))
        curSequence.append((nextTimePoint,ReservedChangeValues.NOT_EXISTANT_CELL))
    } else {
      //we confirm this value, but it is the same as the previous so no need to insert anything new, but we might need to insert wildcard on the next day
      val nextTimePoint = t.plusDays(lowestGranularityInDays)
      if(insertWildcardAfter && !valueConfirmationPoints.contains(nextTimePoint) && v!=ReservedChangeValues.NOT_EXISTANT_CELL && !nextTimePoint.isAfter(InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP))
        curSequence.append((nextTimePoint,ReservedChangeValues.NOT_EXISTANT_CELL))
    }
  }

  def transformGranularityAndExpandTimeRange(mode:WikipediaLineageCreationMode) = {
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
                addValueToSequence(lineage,ld.plusDays(getGracePeriodInDays),ReservedChangeValues.NOT_EXISTANT_CELL,false)
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
                addValueToSequence(lineage,ld.plusDays(getGracePeriodInDays),ReservedChangeValues.NOT_EXISTANT_CELL,false)
            }
          }

      }}
      lineage
        .zipWithIndex
        .foreach(t => {
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
          updateHistoryIfChanged(new InfoboxProperty("extra_link",p + s"_🔗_extractedLink$curLinkPosition"),linkTarget,t)
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

  def toWikipediaInfoboxValueHistories(mode: WikipediaLineageCreationMode) = {
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
    val lineages = transformGranularityAndExpandTimeRange(mode)
      .withFilter(_._2.lineage.values.exists(v => !FactLineage.isWildcard(v)))
      .map(t => {
        (t._1,t._2.toSerializationHelper)
        WikipediaInfoboxValueHistory(revisions.head.template,revisions.head.pageID,revisions.head.key,t._1,t._2.toSerializationHelper)
      })
    lineages
  }

  def getGracePeriodInDays = {
    28
  }

  def changeCount(p:String) = {


  }

}
object InfoboxRevisionHistory extends StrictLogging{

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
