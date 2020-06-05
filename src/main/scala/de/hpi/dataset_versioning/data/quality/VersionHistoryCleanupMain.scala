package de.hpi.dataset_versioning.data.quality

import java.io.{File, PrintWriter}

import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.quality.VersionHistoryCleanupMain.list
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object VersionHistoryCleanupMain extends App {
  IOService.socrataDir = args(0)
  val toIgnoreCountFile = new File(args(1))
  val list = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
  val versionHistoryFile = IOService.getCleanedVersionHistoryFile()

  removeIgnoredVersionsBasedOnMissingCustomMetadata
  //removeIgnoredVersionsBasedOnHash
  //removeIgnoredVersions

  def removeIgnoredVersionsBasedOnMissingCustomMetadata = {
    val list = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
    val shrinkageReportFileWriter = new PrintWriter(toIgnoreCountFile)
    val newList = DatasetVersionHistory.removeIgnoredVersionsBasedOnMissingCustomMetadata(list,shrinkageReportFileWriter)
    shrinkageReportFileWriter.close()
    val pr = new PrintWriter(IOService.getCleanedVersionHistoryFile() + "_cleanedByMissingMetadata")
    newList.foreach(vh => pr.println(vh.toJson))
    pr.close()
  }

  def removeIgnoredVersionsBasedOnHash = {
    DatasetVersionHistory.removeVersionIgnoreBasedOnHashEquality(list,toIgnoreCountFile)
    val pr = new PrintWriter(IOService.getCleanedVersionHistoryFile() + "_cleanedByHash")
    list.foreach(vh => pr.println(vh.toJson))
    pr.close()
  }

  def removeIgnoredVersions = {
    DatasetVersionHistory.removeVersionIgnore(list)
    val pr = new PrintWriter(IOService.getCleanedVersionHistoryFile())
    list.foreach(vh => pr.println(vh.toJson))
    pr.close()
  }

}
