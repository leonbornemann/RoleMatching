package de.hpi.tfm.data.wikipedia.infobox

import de.hpi.tfm.data.socrata.change.{Change, ReservedChangeValues}
import de.hpi.tfm.io.IOService

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

case class InfoboxRevisionHistory(key:String,revisions:collection.Seq[InfoboxRevision]) {
  //May 15, 2012 11:16:19 PM
  def stringToDate(str:String):LocalDateTime = {
    println(str)
    LocalDateTime.parse(str,formatter)
  }
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("MMM d[d], yyyy h[h]:mm:ss a")

  val revisionsSorted = revisions.sortBy(r => stringToDate(r.validFrom))
    .toIndexedSeq

  val lowestGranularityInDays = 1

  val propToValueHistory = collection.mutable.HashMap[String,collection.mutable.ArrayBuffer[(LocalDateTime,String)]]()

  def updateHistory(p: String, newValue: String,t:LocalDateTime) = {
    val curHistory = propToValueHistory.getOrElseUpdate(p,collection.mutable.ArrayBuffer[(LocalDateTime,String)]())
    if(curHistory.isEmpty){
      ???
    } else{
      ???
    }
  }

  def toChangeCube:IndexedSeq[Change] = {
    ???

    //TODO: continue implementation here: construct change cube according to revision data! use integrity constraints checked in RevisionHistory to make things simpler
    revisionsSorted.foreach(r => {
      println(r.revisionType)
      r.changes.foreach(c => {
        val p = c.property
        val e = r.key
        val newValue = if(c.currentValue.isDefined) c.currentValue.get else ReservedChangeValues.NOT_EXISTANT_CELL
        updateHistory(p,newValue,stringToDate(r.validFrom))
        assert(false)
      })
    })
    ???
  }
}
object InfoboxRevisionHistory{
  def EARLIEST_HISTORY_TIMESTAMP = ??? //TODO: find out the date of that LocalDate.parse("2016-11-01", IOService.dateTimeFormatter)
}
