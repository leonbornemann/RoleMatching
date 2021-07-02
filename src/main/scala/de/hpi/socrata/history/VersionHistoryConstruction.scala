package de.hpi.socrata.history

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.diff.syntactic.DiffManager
import de.hpi.socrata.io.Socrata_IOService
import de.hpi.socrata.metadata.custom.joinability.`export`.SnapshotDiff

import java.io.PrintWriter

class VersionHistoryConstruction() extends StrictLogging{

  val diffManager = new DiffManager()

  /***
   * this assumes that no deletes happened!
   */
  def constructVersionHistoryForSimplifiedFiles(writeToCleanedFile:Boolean = true): Unit ={
    val versions = Socrata_IOService.getSortedSimplifiedVersions
    //initialize:
    val idToVersions = scala.collection.mutable.HashMap[String,DatasetVersionHistory]()
    for(i <- 0 until versions.size){
      val curVersion = versions(i)
      val ids = Socrata_IOService.getSimplifiedDatasetIDSInVersion(curVersion)
      ids.foreach(id => {
        val history = idToVersions.getOrElseUpdate(id,new DatasetVersionHistory(id))
        history.versionsWithChanges += curVersion
      })
    }
    val versionHistoryFile = if(writeToCleanedFile) Socrata_IOService.getCleanedVersionHistoryFile() else Socrata_IOService.getVersionHistoryFile()
    val pr = new PrintWriter(versionHistoryFile)
    idToVersions.values.foreach(vh => pr.println(vh.toJson))
    pr.close()
  }

  def constructVersionHistory() = {
    val versions = Socrata_IOService.getSortedDatalakeVersions()
    //initialize:
    val initialFiles = Socrata_IOService.extractDataToWorkingDir(versions(0))
    val idToVersions = scala.collection.mutable.HashMap[String,DatasetVersionHistory]()
    initialFiles.foreach(f => {
      val id = Socrata_IOService.filenameToID(f)
      val history = idToVersions.getOrElseUpdate(id, new DatasetVersionHistory(id))
      history.versionsWithChanges += versions(0)
    })
    for(i <- 1 until versions.size){
      val curVersion = versions(i)
      if(!Socrata_IOService.compressedDiffExists(curVersion) && !Socrata_IOService.uncompressedDiffExists(curVersion)){
        logger.trace(s"Creating Diff for $curVersion")
        diffManager.calculateDiff(curVersion,restorePreviousSnapshotIfNecessary = true)
      }
      Socrata_IOService.extractDiffToWorkingDir(curVersion)
      val snapshotDiff = new SnapshotDiff(curVersion,Socrata_IOService.getUncompressedDiffDir(curVersion))
      (snapshotDiff.createdDatasetIds ++snapshotDiff.changedDatasetIds).foreach(id => {
        val history = idToVersions.getOrElseUpdate(id,new DatasetVersionHistory(id))
        history.versionsWithChanges += curVersion
      })
      snapshotDiff.deletedDatasetIds.foreach(id => {
        val history = idToVersions.getOrElseUpdate(id,new DatasetVersionHistory(id))
        history.deletions += curVersion
      })
      Socrata_IOService.clearUncompressedDiff(curVersion)
    }
    val versionHistoryFile = Socrata_IOService.getVersionHistoryFile()
    val pr = new PrintWriter(versionHistoryFile)
    idToVersions.values.foreach(vh => pr.println(vh.toJson))
    pr.close()
  }


}
