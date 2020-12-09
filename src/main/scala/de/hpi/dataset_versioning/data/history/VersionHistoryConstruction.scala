package de.hpi.dataset_versioning.data.history

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.diff.syntactic.DiffManager
import de.hpi.dataset_versioning.data.metadata.custom.joinability.`export`.SnapshotDiff
import de.hpi.dataset_versioning.io.IOService

import java.io.PrintWriter

class VersionHistoryConstruction() extends StrictLogging{

  val diffManager = new DiffManager()

  /***
   * this assumes that no deletes happened!
   */
  def constructVersionHistoryForSimplifiedFiles(writeToCleanedFile:Boolean = true): Unit ={
    val versions = IOService.getSortedSimplifiedVersions
    //initialize:
    val idToVersions = scala.collection.mutable.HashMap[String,DatasetVersionHistory]()
    for(i <- 0 until versions.size){
      val curVersion = versions(i)
      val ids = IOService.getSimplifiedDatasetIDSInVersion(curVersion)
      ids.foreach(id => {
        val history = idToVersions.getOrElseUpdate(id,new DatasetVersionHistory(id))
        history.versionsWithChanges += curVersion
      })
    }
    val versionHistoryFile = if(writeToCleanedFile) IOService.getCleanedVersionHistoryFile() else IOService.getVersionHistoryFile()
    val pr = new PrintWriter(versionHistoryFile)
    idToVersions.values.foreach(vh => pr.println(vh.toJson))
    pr.close()
  }

  def constructVersionHistory() = {
    val versions = IOService.getSortedDatalakeVersions()
    //initialize:
    val initialFiles = IOService.extractDataToWorkingDir(versions(0))
    val idToVersions = scala.collection.mutable.HashMap[String,DatasetVersionHistory]()
    initialFiles.foreach(f => {
      val id = IOService.filenameToID(f)
      val history = idToVersions.getOrElseUpdate(id, new DatasetVersionHistory(id))
      history.versionsWithChanges += versions(0)
    })
    for(i <- 1 until versions.size){
      val curVersion = versions(i)
      if(!IOService.compressedDiffExists(curVersion) && !IOService.uncompressedDiffExists(curVersion)){
        logger.trace(s"Creating Diff for $curVersion")
        diffManager.calculateDiff(curVersion,restorePreviousSnapshotIfNecessary = true)
      }
      IOService.extractDiffToWorkingDir(curVersion)
      val snapshotDiff = new SnapshotDiff(curVersion,IOService.getUncompressedDiffDir(curVersion))
      (snapshotDiff.createdDatasetIds ++snapshotDiff.changedDatasetIds).foreach(id => {
        val history = idToVersions.getOrElseUpdate(id,new DatasetVersionHistory(id))
        history.versionsWithChanges += curVersion
      })
      snapshotDiff.deletedDatasetIds.foreach(id => {
        val history = idToVersions.getOrElseUpdate(id,new DatasetVersionHistory(id))
        history.deletions += curVersion
      })
      IOService.clearUncompressedDiff(curVersion)
    }
    val versionHistoryFile = IOService.getVersionHistoryFile()
    val pr = new PrintWriter(versionHistoryFile)
    idToVersions.values.foreach(vh => pr.println(vh.toJson))
    pr.close()
  }


}
