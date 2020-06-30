package de.hpi.dataset_versioning.db_synthesis.top_down.decomposition

import java.io.PrintWriter
import java.time.LocalDate

import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable.ArrayBuffer

object ChangeHistogramBySubdomainMain extends App {

  IOService.socrataDir = args(0)
  def read = {
    val changeCounts = Seq(0,1,2,3,4,5,10,15,20)
    println(s"subdomain,#datasets,${changeCounts.map("nChange >= "+_).mkString(",")}")
    val datasetInfosBySubDomain = DatasetInfo.readDatasetInfoBySubDomain
    datasetInfosBySubDomain
      .map{case (k,v) => (k,v.size,changeCounts.map(cc => v.filter(_.numChanges>= cc).size))}
      .toIndexedSeq
      .sortBy(-_._3.last)
      .foreach{case (k,v,vec) => println(s"$k,$v,${vec.mkString(",")}")}
  }

  read
  //write

  private def write = {
    IOService.socrataDir = args(0)
    var histories = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
    val idToVersions = histories.map(h => (h.id, h))
      .toMap
    val md = IOService.getOrLoadCustomMetadataForStandardTimeFrame()
    var byDomain = idToVersions
      .groupBy { case (k, list) => {
        val allVersions = list.versionsWithChanges
        val firstVersion = DatasetInstance(k, allVersions.head)
        if (!md.metadata.contains(firstVersion))
          null
        else
          md.metadata(firstVersion).topLevelDomain
      }
      }
    val pr = new PrintWriter("changeCountWithSubdomain.csv")
    pr.println("subdomain,id,#changes,#deletes,firstInsert,latestChange,latestDelete")

    byDomain.foreach{case (k,map) => {
      map.foreach(entry => {
        val changes = entry._2.versionsWithChanges
        val deletions = entry._2.deletions
        val firstInsert = firstOrNa(changes.sortBy(_.toEpochDay))
        val latestChange = lastOrNa(changes.sortBy(_.toEpochDay))
        val latestDelete = lastOrNa(deletions.sortBy(_.toEpochDay))
        pr.println(s"$k,${entry._1},${changes.size-1},${deletions.size},$firstInsert,$latestChange,$latestDelete}")
      })
    }}
    pr.close()
  }

  def lastOrNa(value: ArrayBuffer[LocalDate]) = {
    if(value.isEmpty) "NA" else value.last.format(IOService.dateTimeFormatter)
  }

  def firstOrNa(value: ArrayBuffer[LocalDate]) = if(value.isEmpty) "NA" else value.head.format(IOService.dateTimeFormatter)

}
