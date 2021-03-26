package de.hpi.tfm.io

import com.typesafe.scalalogging.StrictLogging

import java.time.LocalDate
import java.time.format.DateTimeParseException

object InteractiveSaveDeleteMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  var done = false

  def deleteUncompressedDiff(version: LocalDate) = {
    logger.trace(s"Trying to delete uncompressed diff in version $version")
    if (!IOService.uncompressedDiffExists(version)) {
      logger.trace(s"no uncompressed diff in version $version exists")
    } else if (!IOService.compressedDiffExists(version)) {
      logger.trace(s"compressed Snapshot in version $version does not exist - not deleting, this should be fixed")
    } else {
      logger.debug("Beginning Deletion")
      IOService.clearUncompressedDiff(version)
      logger.trace("Deletion complete")
    }
  }

  while (!done) {
    IOService.printSummary()
    logger.debug("Type in <us> for uncompressed snapshot, cs for compressed snapshot, ud for uncompressed diff, anything else to quit")
    val mode = scala.io.StdIn.readLine()
    if (!Seq("us", "cs", "ud").contains(mode)) {
      done = true
    } else {
      logger.debug("Type in date to delete the uncompressed snapshot of, or q to quit")
      val input = scala.io.StdIn.readLine()
      if (input.toLowerCase == "q"){
        done = true
      } else {
        var version = Seq[LocalDate]()
        try {
          version = input.split(",").map(s => LocalDate.parse(s.trim, IOService.dateTimeFormatter))
        } catch {
          case e: DateTimeParseException => logger.debug("No valid version enterered")
        }
        if (mode == "us" && !version.isEmpty) {
          logger.trace(s"Trying to delete $version")
          version.foreach(v => deleteUncompressedSnapshot(v))
        } else if (mode == "cs" && !version.isEmpty) {
          version.foreach(v => deleteCompressedSnapshot(v))
        } else if (mode == "ud" && !version.isEmpty) {
          version.foreach(v => deleteUncompressedDiff(v))
        }
      }
    }
  }

  private def deleteCompressedSnapshot(version: LocalDate) = {
    logger.trace(s"Trying to delete compressed snapshot in version $version")
    if (!IOService.compressedSnapshotExists(version)) {
      logger.trace(s"no compressed Snapshot in version $version exists")
    } else if (IOService.shouldBeCheckpoint(version)) {
      logger.trace(s"compressed Snapshot in version $version exists but is checkpoint - not deleting")
    } else {
      logger.debug("Beginning Deletion")
      IOService.saveDeleteCompressedDataFile(version)
      logger.trace("Deletion complete")
    }
  }

  private def deleteUncompressedSnapshot(version: LocalDate) = {
    logger.trace(s"Trying to delete uncompressed snapshot in version $version")
    val checkpoints = IOService.getCheckpoints()
    if (!IOService.uncompressedSnapshotExists(version)) {
      logger.trace(s"no uncompressed Snapshot in version $version exists")
    } else if (checkpoints.indexOf(version) <= 0 && !IOService.compressedSnapshotExists(version)) {
      logger.debug(s"Can't safely delete $version - it is the first checkpoint and no compressed snapshot exists - this should be fixed")
    } else {
      logger.debug("Beginning Deletion")
      IOService.clearUncompressedSnapshot(version)
      logger.trace("Deletion complete")
    }
  }
}
