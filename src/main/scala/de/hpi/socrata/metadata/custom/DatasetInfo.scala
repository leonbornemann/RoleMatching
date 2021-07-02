package de.hpi.socrata.metadata.custom

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.io.Socrata_IOService

import java.time.LocalDate
import java.time.format.DateTimeParseException
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

  def getViewIdsInSubdomain(subdomain:String) = {
    readDatasetInfoBySubDomain(subdomain)
      .map(_.id)
      .toIndexedSeq
  }

  def date(str: String): Option[LocalDate] = {
    try {
      Some(LocalDate.parse(str, Socrata_IOService.dateTimeFormatter))
    } catch {
      case e:DateTimeParseException => None
    }
  }

  def fromLine(l: String): DatasetInfo = {
    val toks = l.split(",")
    DatasetInfo(toks(0),toks(1),toks(2).toInt,toks(3).toInt,date(toks(4)),date(toks(5)),date(toks(6).substring(0,toks(6).size-1)))
  }

  def readDatasetInfoBySubDomain = {
    val idsToFilter = Socrata_IOService.getIdsToFilterOut()
    Source.fromFile(Socrata_IOService.getDatasetInfoFile)
      .getLines()
      .toSeq
      .tail
      .map(l => DatasetInfo.fromLine(l))
      .filter(di => !idsToFilter.contains(di.id))
      .groupBy(_.subdomain)
  }
}
