package de.hpi.socrata.diff.syntactic

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.io.{IOUtil, Socrata_IOService}
import de.hpi.socrata.metadata.custom.joinability.`export`.SnapshotDiff

import java.io.File
import java.time.LocalDate

class DiffManager(daysBetweenCheckpoints:Int=7) extends StrictLogging{

  def replaceAllNonCheckPointsWithDiffs(tmpDirectory:File,calculateDiffForAll:Boolean=true,batchMode:Boolean=false) = {
    val versions = Socrata_IOService.getSortedDatalakeVersions()
    val actions = (0 until versions.size).map(i => {
      if(!isCheckpoint(i) && Socrata_IOService.compressedDiffExists(versions(i)))
        (versions(i),"Keep compressed Diff as is")
      else if(!isCheckpoint(i) && !Socrata_IOService.compressedDiffExists(versions(i)))
        (versions(i),"Replace with compressed Diff")
      else if(isCheckpoint(i) && calculateDiffForAll && !Socrata_IOService.compressedDiffExists(versions(i)) && Socrata_IOService.getSortedDatalakeVersions.head!=versions(i))
        (versions(i),"Calculate Diff for checkpoint, but keep checkpoint")
      else
        (versions(i),"Keep as Checkpoint")
    })
    logger.debug("DiffManager is about to perform the following actions:")
    actions.groupBy(a =>a._2)
        .toSeq
        .sortBy(_._2.head._1.toEpochDay)
        .foreach{ case(action,versions) => logger.debug(s"$action: ${versions.map(_._1)}")}
    var input = ""
    if(!batchMode){
      logger.debug("Enter y to continue, anything else to exit")
      input = scala.io.StdIn.readLine()
    }
    if(batchMode || input.toLowerCase() == "y") {
      for(i <- 1 until versions.size) {
        val version = versions(i)
        if((calculateDiffForAll || !isCheckpoint(i)) && !Socrata_IOService.compressedDiffExists(version) && Socrata_IOService.getSortedDatalakeVersions.head!=version){
          replaceVersionWithDiff(tmpDirectory, i, version)
        }
      }
    } else{
      logger.debug("terminating")
    }
  }

  def replaceSingleVersionWithDiff(tmpDirectory: File, version: LocalDate) = {
    val versions = Socrata_IOService.getSortedDatalakeVersions()
    val i = versions.indexOf(version)
    replaceVersionWithDiff(tmpDirectory,i,version)
    if (Socrata_IOService.uncompressedSnapshotExists(version.minusDays(1))) {
      Socrata_IOService.clearUncompressedSnapshot(version.minusDays(1))
      Socrata_IOService.clearUncompressedDiff(version.minusDays(1))
    }
  }

  private def replaceVersionWithDiff(tmpDirectory: File, i: Int, version: LocalDate) = {
    logger.debug(s"Starting replacement of $version")
    logger.debug(s"Calculating Diff")
    calculateDiff(version)
    logger.debug(s"Testing Snapshot Restore in temporary Directory")
    restoreFullSnapshotFromDiff(version, Some(tmpDirectory))
    Socrata_IOService.extractDataToWorkingDir(version)
    val uncompressedDir = Socrata_IOService.getUncompressedDataDir(version)
    if (IOUtil.dirEquals(tmpDirectory, uncompressedDir)) {
      logger.debug(s"Snapshot Restore successful - deleting zipped files")
      if (!isCheckpoint(i))
        Socrata_IOService.getCompressedDataFile(version).delete()
    } else {
      throw new AssertionError(s"Restored Directory ${tmpDirectory.getAbsolutePath} contents do not match original ($uncompressedDir) - aborting")
    }
    if (Socrata_IOService.uncompressedSnapshotExists(version.minusDays(2))) {
      Socrata_IOService.clearUncompressedSnapshot(version.minusDays(2))
      Socrata_IOService.clearUncompressedDiff(version.minusDays(2))
    }
    if (Socrata_IOService.uncompressedSnapshotExists(version.minusDays(3))) {
      //happens if we passed a checkpoint
      Socrata_IOService.clearUncompressedSnapshot(version.minusDays(3))
      Socrata_IOService.clearUncompressedDiff(version.minusDays(3))
    }
    logger.debug(s"Cleaning up temporary Directory")
    IOUtil.clearDirectoryContent(tmpDirectory)
  }

  def isCheckpoint(i: Int): Boolean = i % daysBetweenCheckpoints==0
  val diffCalculator = new DiffCalculator

  def calculateDiff(version:LocalDate,deleteUncompressed:Boolean = true,restorePreviousSnapshotIfNecessary:Boolean=true) = {
    if(!Socrata_IOService.compressedDiffExists(version)){
      val diffDir = Socrata_IOService.getUncompressedDiffDir(version)
      if(diffDir.exists()){
        IOUtil.clearDirectoryContent(diffDir)
      }
      diffDir.mkdirs()
      diffCalculator.calculateDiff(version,deleteUncompressed,restorePreviousSnapshotIfNecessary)
    } else{
      logger.debug(s"Skipping diff for version $version because it already exists")
    }
  }

  def restoreMinimalSnapshot(version:LocalDate,deleteUncompressedDiffAfter:Boolean = true) = {
    if(Socrata_IOService.minimalUncompressedVersionDirExists(version)){
      logger.debug(s"skipping minimal snapshot restore of version $version because it already exists")
    } else {
      logger.debug(s"beginning minimal snapshot restore of version $version")
      Socrata_IOService.extractDiffToWorkingDir(version)
      val diff = new SnapshotDiff(version, Socrata_IOService.getUncompressedDiffDir(version))
      val dstDir = Socrata_IOService.getMinimalUncompressedVersionDir(version)
      //copy created files
      diff.createdDatasetFiles.foreach(f => {
        val dst = new File(dstDir + "/" + f.getName)
        java.nio.file.Files.copy(f.toPath, dst.toPath)
      })
      logger.debug("finished copying created files")
      //TODO: create a list of deleted files (?) - would be an integrity check only
      //patch changed files
      diff.diffFiles.foreach(diffFile => {
        //find latest version:
        val srcFile = getLatestVersionBefore(version, Socrata_IOService.filenameToID(diffFile)) //TODO: weird output here+ IOService.filenameToID(diffFile)
        diffCalculator.patchFile(dstDir, diffFile, srcFile.getName, srcFile.getAbsolutePath)
      })
      logger.debug(s"Finished minimal snapshot restore for version $version")
    }
    if(deleteUncompressedDiffAfter){
      logger.debug(s"Deleting uncompressed Diff for $version")
      Socrata_IOService.clearUncompressedDiff(version)
    }
    Socrata_IOService.getMinimalUncompressedVersionDir(version).listFiles
  }

  def getLatestVersionBefore(version: LocalDate, id: String) = {
    val versions = Socrata_IOService.getSortedMinimalUmcompressedVersions
      .filter(v => {
      val containsFileID = Socrata_IOService.getMinimalUncompressedVersionDir(v)
        .listFiles()
        .map(Socrata_IOService.filenameToID(_))
        .contains(id)
      v.toEpochDay < version.toEpochDay && containsFileID
    })
    new File(s"${Socrata_IOService.getMinimalUncompressedVersionDir(versions.last).getAbsolutePath}/$id.json?")
  }

  def restoreFullSnapshotFromDiff(version:LocalDate, targetDir:Option[File] = None,recursivelyRestoreSnapshots:Boolean = false):Unit = {
    if(Socrata_IOService.compressedSnapshotExists(version)){
      logger.trace(s"Skipping restore of ${version} because it already exists")
    } else if(Socrata_IOService.uncompressedSnapshotExists(version) && !targetDir.isDefined){
      logger.trace(s"Not restoring ${version} from Diff because uncompressed Snapshot exists for it - Compressed Snapshot will be created from uncompressed File.")
      Socrata_IOService.compressDataFromWorkingDir(version)
    } else{
      assert(Socrata_IOService.compressedDiffExists(version))
      if(!recursivelyRestoreSnapshots && !Socrata_IOService.snapshotExists(version.minusDays(1))){
        throw new AssertionError(s"no snapshot available for ${version.minusDays(1)}")
      } else if(Socrata_IOService.snapshotExists(version.minusDays(1))) {
        Socrata_IOService.extractDataToWorkingDir(version.minusDays(1))
        Socrata_IOService.extractDiffToWorkingDir(version)
        if(!targetDir.isDefined)
          Socrata_IOService.getUncompressedDataDir(version).mkdirs()
        diffCalculator.recreateFromDiff(version,targetDir)
      } else{
        assert(recursivelyRestoreSnapshots)
        logger.debug(s"recursively restoring ${version.minusDays(1)}")
        restoreFullSnapshotFromDiff(version.minusDays(1),recursivelyRestoreSnapshots=true)
        //TODO: fix code copy paste?
        Socrata_IOService.extractDataToWorkingDir(version.minusDays(1))
        Socrata_IOService.extractDiffToWorkingDir(version)
        if(!targetDir.isDefined)
          Socrata_IOService.getUncompressedDataDir(version).mkdirs()
        diffCalculator.recreateFromDiff(version,targetDir)
        Socrata_IOService.clearUncompressedSnapshot(version.minusDays(1))
        Socrata_IOService.saveDeleteCompressedDataFile(version.minusDays(1))
      }
    }
  }

  def calculateAllDiffs() = {
    val versions = Socrata_IOService.getSortedDatalakeVersions()
    //start at 1 because origin must be kept anyway
    for(i <- 0 until versions.size) {
      if(!isCheckpoint(i)){
        calculateDiff(versions(i))
      }
    }
  }
}
