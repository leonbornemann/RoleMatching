package de.hpi.tfm.data.socrata.metadata.custom.joinability.`export`

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.DatasetInstance
import de.hpi.tfm.data.socrata.diff.syntactic.DiffManager
import de.hpi.tfm.io.IOService

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class LSHEnsembleDomainExporter() extends StrictLogging{

  val diffManager = new DiffManager()

  def exportFiles(files: Iterable[File],version:LocalDate,outDir:File) = {
    logger.debug(s"Beginning domain export for ${version}_lshensemble_domains.json")
    val writer = new PrintWriter(outDir.getAbsolutePath + s"/${IOService.dateTimeFormatter.format(version)}")
    var count = 0
    files.foreach(f => {
      if(count%1000==0) logger.debug(s"Finisihed domain export of $count datasets")
      val ds = IOService.tryLoadDataset(new DatasetInstance(IOService.filenameToID(f),version),minimal = true)
      ds.exportColumns(writer)
      count+=1
    })
    writer.close()
    logger.debug(s"FInished domain export for $version")
  }

  def export(startVersion: LocalDate, endVersion:LocalDate, outDir: File,reExportStartVersion:Boolean = false) = {
    //first snapshot full:
    if(reExportStartVersion) {
      val files = IOService.extractDataToMinimalWorkingDir(startVersion) // IOService.extractDataToWorkingDir(startVersion)
      exportFiles(files, startVersion, outDir)
    }
    //other snapshots:
    val allVersions = IOService.getSortedDatalakeVersions()
    val versionsToExport = ArrayBuffer[LocalDate]() ++ allVersions.slice(allVersions.indexOf(startVersion)+1,allVersions.indexOf(endVersion)+1)
    versionsToExport.foreach(v => {
      logger.debug(s"exporting LSH domains for version $v")
      diffManager.restoreMinimalSnapshot(v)
      val datasets = IOService.getMinimalUncompressedVersionDir(v)
        .listFiles
        .filter(_.getName.endsWith(".json?"))
      exportFiles(datasets,v,outDir)
    })
  }

  def removeWhile(list:ArrayBuffer[LocalDate],f:LocalDate => Boolean):ArrayBuffer[LocalDate] = {
    val newList = list.takeWhile(f)
    list.dropWhile(f)
    newList
  }
}
