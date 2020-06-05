package de.hpi.dataset_versioning.data.quality

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

object VersionIgnoreFinder extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val versionToIgnore = Seq("2019-11-18","2019-12-23","2020-01-20","2020-02-17","2020-03-07","2020-03-14","2020-03-21","2020-03-28","2020-04-04","2020-04-11","2020-04-18","2020-04-20")
    .map(LocalDate.parse(_,IOService.dateTimeFormatter))
  val toIgnorePr = new PrintWriter(IOService.getVersionIgnoreFile())
  toIgnorePr.println("id,version")
  versionToIgnore.foreach(v => {
    logger.trace(s"Processing $v")
    val prevVersion = v.minusDays(1)
    IOService.extractDiffToWorkingDir(prevVersion)
    IOService.extractDiffToWorkingDir(v)
    val prevDeletedFile = new File(IOService.getUncompressedDiffDir(prevVersion).getAbsolutePath + "/deleted.meta")
    val deletedInPrevious = if(prevDeletedFile.exists()) Source.fromFile(prevDeletedFile).getLines().toSet.map((s:String) => IOService.filenameToID(new File(s))) else Set[String]()
    val currentCreatedFile = new File(IOService.getUncompressedDiffDir(v).getAbsolutePath + "/created.meta")
    val createdInCurrent = if(currentCreatedFile.exists()) Source.fromFile(currentCreatedFile).getLines().toSet.map((s:String) => IOService.filenameToID(new File(s))) else Set[String]()
    logger.trace(s"Found ${deletedInPrevious.size} deleted datasets and ${createdInCurrent.size} created datasets")
    deletedInPrevious.intersect(createdInCurrent).foreach(id => {
      toIgnorePr.println(s"$id,${v.format(IOService.dateTimeFormatter)}")
    })
    IOService.clearUncompressedDiff(v)
    IOService.clearUncompressedDiff(prevVersion)
  })
  toIgnorePr.close()
  //TODO: version 2019-12-23 is weird - has created, previous does not have deleted?
}
