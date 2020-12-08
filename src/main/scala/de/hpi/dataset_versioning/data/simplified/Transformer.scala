package de.hpi.dataset_versioning.data.simplified

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

class Transformer() extends StrictLogging{

  def transformAllFromErrorFile(errorFilePath: String) = {
    val errorFile = new PrintWriter("errorsNew.txt")
    val instances = Source.fromFile(errorFilePath).getLines().toSeq
      .tail
      .map(s => s.substring(1,s.size-1))
      .map(s => DatasetInstance(s.split(",")(0),LocalDate.parse(s.split(",")(1),IOService.dateTimeFormatter)))
    instances.foreach{case DatasetInstance(id,version) => {
      tryTransformVersion(id,Some(errorFile),version)
    }}
    errorFile.close()
  }


  def logProgress(count: Int, size: Int, modulo: Int) = {
    if (count % modulo==0)
      logger.debug(s"Finished $count out of $size (${100.0*count/size}%)")
  }

  def transformAll(timeRange:Option[(LocalDate,LocalDate)]=None) = {
    val errorFile = new PrintWriter("errors.txt")
    errorFile.println("id","version")
    var count = 0
    val versionHistories = DatasetVersionHistory.load()
      .map(h => (h.id,h))
    versionHistories.foreach{case (id,h) => {
      transformAllVersions(id,h.versionsWithChanges.toIndexedSeq,Some(errorFile),timeRange)
      count+=1
      logProgress(count,versionHistories.size,100)
    }}
    errorFile.close()
  }

  def transformAllForID(id: String,timeRange:Option[(LocalDate,LocalDate)]=None) = {
    val versions = IOService.getSortedMinimalUmcompressedVersions
      .filter(d => {
        val fileExsists = IOService.getMinimalUncompressedVersionDir(d).listFiles().exists(f => {
          f.getName == IOService.jsonFilenameFromID(id)
        })
        val versionInRange = !timeRange.isDefined || d.toEpochDay >= timeRange.get._1.toEpochDay && d.toEpochDay <= timeRange.get._2.toEpochDay
        fileExsists && versionInRange
      })
    transformAllVersions(id, versions)
  }

  private def transformAllVersions(id: String,
                                   versions: IndexedSeq[LocalDate],
                                   errorLog:Option[PrintWriter] = None,
                                   timeRange:Option[(LocalDate,LocalDate)]=None) = {
    for (version <- versions) {
      if(!timeRange.isDefined || version.toEpochDay >= timeRange.get._1.toEpochDay && version.toEpochDay <= timeRange.get._2.toEpochDay)
        tryTransformVersion(id, errorLog, version)
    }
  }

  private def tryTransformVersion(id: String, errorLog: Option[PrintWriter], version: LocalDate) = {
    try {
      transformVersion(id, version)
    } catch {
      case e: Throwable =>
        logError(id, errorLog, version)
    }
  }

  private def logError(id: String, errorLog: Option[PrintWriter], version: LocalDate) = {
    if (errorLog.isDefined) {
      errorLog.get.println(id, version)
      errorLog.get.flush()
    }
  }

  def transformVersion(id: String, version: LocalDate) = {
    val ds = IOService.tryLoadDataset(DatasetInstance(id, version), true)
    val improved = ds.toImproved
    val outDir = IOService.getSimplifiedDataDir(version)
    val outFile = new File(outDir.getAbsolutePath + s"/$id.json?")
    improved.toJsonFile(outFile)
  }
}
