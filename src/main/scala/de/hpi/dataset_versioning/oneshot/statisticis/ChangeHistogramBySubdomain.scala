package de.hpi.dataset_versioning.oneshot.statisticis

import java.io.PrintWriter
import java.time.LocalDate

import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.exploration.db_synthesis.ConditionalProbabilityExplorationMain.histories
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable.ArrayBuffer

object ChangeHistogramBySubdomain extends App {

  IOService.socrataDir = args(0)
  var histories = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
  val idToVersions = histories.map(h => (h.id,h))
    .toMap
  val md = IOService.getOrLoadCustomMetadataForStandardTimeFrame()
  var byDomain = idToVersions
    .groupBy{case (k,list) => {
      val allVersions = list.versionsWithChanges
      val firstVersion = DatasetInstance(k, allVersions.head)
      if(!md.metadata.contains(firstVersion))
        null
      else
        md.metadata(firstVersion).topLevelDomain
    }}
  val pr = new PrintWriter("changeCountWithSubdomain.csv")
  pr.println("subdomain,id,#changes,#deletes,firstInsert,latestChange,latestDelete")

  def lastOrNan(value: ArrayBuffer[LocalDate]) = {
    if(value.isEmpty) "NA" else value.last.format(IOService.dateTimeFormatter)
  }

  def firstOrNan(value: ArrayBuffer[LocalDate]) = if(value.isEmpty) "NA" else value.head.format(IOService.dateTimeFormatter)

  byDomain.foreach{case (k,map) => {
    map.foreach(entry => {
      val changes = entry._2.versionsWithChanges
      val deletions = entry._2.deletions
      val firstInsert = firstOrNan(changes.sortBy(_.toEpochDay))
      val latestChange = lastOrNan(changes.sortBy(_.toEpochDay))
      val latestDelete = lastOrNan(deletions.sortBy(_.toEpochDay))
      pr.println(s"$k,${entry._1},${changes.size-1},${deletions.size},$firstInsert,$latestChange,$latestDelete}")
    })
  }}
  pr.close()
}
