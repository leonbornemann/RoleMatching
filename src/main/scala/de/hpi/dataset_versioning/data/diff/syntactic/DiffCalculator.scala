package de.hpi.dataset_versioning.data.diff.syntactic

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._
class DiffCalculator() extends StrictLogging{



  def getLines(file: File) = Source.fromFile(file).getLines().toSet

  def isDiffFile(f: File): Boolean = f.getName.endsWith(".diff")

  def isDataFile(f: File): Boolean = f.getName.endsWith(".json?")

  def diffToOriginalName(diffFilename: String) = diffFilename.substring(0,diffFilename.lastIndexOf('.'))

  def recreateFromDiff(version:LocalDate, externalTarget:Option[File] = None) = {
    val uncompressedDiffDir = IOService.getUncompressedDiffDir(version)
    val uncompressedPreviousVersionDir = IOService.getUncompressedDataDir(version.minusDays(1))
    val target = if(externalTarget.isDefined) externalTarget.get else IOService.getUncompressedDataDir(version)
    val processedFiles = scala.collection.mutable.HashSet[String]()
    uncompressedDiffDir
      .listFiles()
      .foreach(f => {
        if(isDiffFile(f)){
          //path the changed files
          val diffFilename = f.getName
          val originalName = diffToOriginalName(diffFilename)
          processedFiles += originalName
          val originalFilepath = uncompressedPreviousVersionDir.getAbsolutePath + "/" + originalName
          patchFile(target, f, originalName, originalFilepath)
        } else if(isDataFile(f)){
          //copy all created files (they are in the diff):
          val toExecute = s"cp ${f.getAbsolutePath} ${target.getAbsolutePath}"
          toExecute!
        } else if (f.getName=="deleted.meta") {
          processedFiles ++= Source.fromFile(f).getLines()
        }
        else{
          println("skipping meta file " +f.getName)
        }
      })
    //copy all unchanged files from the original directory
    uncompressedPreviousVersionDir.listFiles()
      .filter(f => !processedFiles.contains(f.getName))
      .foreach(f => {
        val toExecute = s"cp ${f.getAbsolutePath} ${target.getAbsolutePath}"
        toExecute!
      })
    if(!externalTarget.isDefined)
      IOService.compressDataFromWorkingDir(version)
  }

  def patchFile(targetDir: File, diffFile: File, originalFileName: String, originalFilepath: String) = {
    val toExecute = s"patch --quiet -o ${targetDir.getAbsolutePath + "/" + originalFileName} $originalFilepath ${diffFile.getAbsolutePath}"
    toExecute ! //TODO: deal with rejects!
  }

  def deleteUnmeaningfulDiffs(diffDirectory:File) = {
    diffDirectory.listFiles()
      .filter(_.length()==0)
      .foreach(_.delete())
  }

  def calculateAllDiffsFromUncompressed(from: LocalDate, to: LocalDate,deleteUncompressed:Boolean) = {
    val filesFrom = IOService.extractDataToWorkingDir(from)
    val filesTo = IOService.extractDataToWorkingDir(to)
    val namesFrom = filesFrom.map(f => (f.getName,f)).toMap
    val namesTo = filesTo.map(f => (f.getName,f)).toMap
    //diffs and deleted files:
    val diffDirectory = IOService.getUncompressedDiffDir(to)
    assert(diffDirectory.exists() && diffDirectory.listFiles.size==0)
    val deleted = new PrintWriter(diffDirectory.getAbsolutePath + "/deleted.meta")
    val created = new PrintWriter(diffDirectory.getAbsolutePath + "/created.meta")
    namesFrom.values.foreach(f => {
      if(namesTo.contains(f.getName)){
        val f2 = namesTo(f.getName)
        val targetFile = new File(diffDirectory.getAbsolutePath + s"/${f.getName}.diff")
        val toExecute = s"diff ${f.getAbsolutePath} ${f2.getAbsolutePath}"
        val targetFilePath = targetFile.getAbsolutePath
        (toExecute #> new File(targetFilePath)).!
      } else{
        deleted.println(f.getName)
      }
    })
    deleted.close()
    //newly created files:
    namesTo.keySet.diff(namesFrom.keySet).foreach(f => {
      val file = namesTo(f)
      created.println(f)
      val toExecute = s"cp ${file.getAbsolutePath} ${diffDirectory.getAbsolutePath}"
      toExecute!
    })
    created.close()
    deleteUnmeaningfulDiffs(diffDirectory)
    //zip the resulting directory:
    IOService.compressDiffFromWorkingDir(to)
    if(deleteUncompressed)
      IOService.clearUncompressedDiff(to)
      //new Directory(diffDirectory).deleteRecursively()
  }

  def calculateDiff(version:LocalDate,deleteUncompressed:Boolean=true,restorePreviousSnapshotIfNecessary:Boolean=true) = {
    logger.trace("calculating diff for {}",version)
    IOService.extractDataToWorkingDir(version)
    val previousVersion = version.minusDays(1)
    if(!restorePreviousSnapshotIfNecessary && !IOService.snapshotExists(previousVersion)){
      throw new AssertionError(s"snapshot for $previousVersion does not exist")
    }
    if(restorePreviousSnapshotIfNecessary && !IOService.snapshotExists(previousVersion)){
      new DiffManager().restoreFullSnapshotFromDiff(previousVersion,recursivelyRestoreSnapshots = true)
    }
    if(!IOService.uncompressedSnapshotExists(previousVersion)){
      IOService.extractDataToWorkingDir(previousVersion)
    }
    calculateAllDiffsFromUncompressed(previousVersion,version,deleteUncompressed)
  }


}
