package de.hpi.socrata.diff.syntactic

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.io.Socrata_IOService

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
    val uncompressedDiffDir = Socrata_IOService.getUncompressedDiffDir(version)
    val uncompressedPreviousVersionDir = Socrata_IOService.getUncompressedDataDir(version.minusDays(1))
    val target = if(externalTarget.isDefined) externalTarget.get else Socrata_IOService.getUncompressedDataDir(version)
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
      Socrata_IOService.compressDataFromWorkingDir(version)
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
    val filesFrom = Socrata_IOService.extractDataToWorkingDir(from)
    val filesTo = Socrata_IOService.extractDataToWorkingDir(to)
    val namesFrom = filesFrom.map(f => (f.getName,f)).toMap
    val namesTo = filesTo.map(f => (f.getName,f)).toMap
    //diffs and deleted files:
    val diffDirectory = Socrata_IOService.getUncompressedDiffDir(to)
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
    Socrata_IOService.compressDiffFromWorkingDir(to)
    if(deleteUncompressed)
      Socrata_IOService.clearUncompressedDiff(to)
      //new Directory(diffDirectory).deleteRecursively()
  }

  def calculateDiff(version:LocalDate,deleteUncompressed:Boolean=true,restorePreviousSnapshotIfNecessary:Boolean=true) = {
    logger.trace("calculating diff for {}",version)
    Socrata_IOService.extractDataToWorkingDir(version)
    val previousVersion = version.minusDays(1)
    if(!restorePreviousSnapshotIfNecessary && !Socrata_IOService.snapshotExists(previousVersion)){
      throw new AssertionError(s"snapshot for $previousVersion does not exist")
    }
    if(restorePreviousSnapshotIfNecessary && !Socrata_IOService.snapshotExists(previousVersion)){
      new DiffManager().restoreFullSnapshotFromDiff(previousVersion,recursivelyRestoreSnapshots = true)
    }
    if(!Socrata_IOService.uncompressedSnapshotExists(previousVersion)){
      Socrata_IOService.extractDataToWorkingDir(previousVersion)
    }
    calculateAllDiffsFromUncompressed(previousVersion,version,deleteUncompressed)
  }


}
