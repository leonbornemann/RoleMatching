package de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition

import java.time.LocalDate
import java.time.format.DateTimeParseException

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

case class DatasetInfo(subdomain:String,id:String,numChanges:Int,numDeletes:Int,firstInsert:Option[LocalDate],latestChange:Option[LocalDate],latestDelete:Option[LocalDate]) extends StrictLogging{
  if(!latestChange.isDefined || !firstInsert.isDefined)
    logger.debug("Warning: missing first version in datasetinfo")

  def getLatestVersion: LocalDate = if(latestChange.isDefined) latestChange.get else firstInsert.get

  def isNotDeletedAtFinalTimestamp: Boolean = {
    if(!latestDelete.isDefined)
      true
    else if(!latestChange.isDefined)
      false
    else
      latestChange.get.isAfter(latestDelete.get)
  }

}

object DatasetInfo {

  def date(str: String): Option[LocalDate] = {
    try {
      Some(LocalDate.parse(str, IOService.dateTimeFormatter))
    } catch {
      case e:DateTimeParseException => None
    }
  }

  def fromLine(l: String): DatasetInfo = {
    val toks = l.split(",")
    DatasetInfo(toks(0),toks(1),toks(2).toInt,toks(3).toInt,date(toks(4)),date(toks(5)),date(toks(6).substring(0,toks(6).size-1)))
  }

  def readDatasetInfoBySubDomain = {
    Source.fromFile(IOService.getDatasetInfoFile)
      .getLines()
      .toSeq
      .tail
      .map(l => DatasetInfo.fromLine(l))
      .groupBy(_.subdomain)
  }
}
