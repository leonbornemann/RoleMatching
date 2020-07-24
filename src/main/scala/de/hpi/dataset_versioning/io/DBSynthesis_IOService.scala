package de.hpi.dataset_versioning.io

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.data.DatasetInstance

object DBSynthesis_IOService {
  def getFDFile(subdomain:String,id: String, date: LocalDate) = new File(s"$FDDIR/$subdomain/$id/${IOService.dateTimeFormatter.format(date)}.csv-hyfd.txt")
  def getExportedCSVFile(subdomain:String,id: String, date: LocalDate) = new File(s"$DECOMPOSITION_EXPORT_CSV_DIR/$subdomain/$id/${IOService.dateTimeFormatter.format(date)}.csv")

  def getExportedCSVSubdomainDir(subdomain:String) = new File(s"$DECOMPOSITION_EXPORT_CSV_DIR/$subdomain/")
  def getMeasurementsDir(subdomain:String) = new File(s"$DECOMPOSITION_MEASUREMENTS_DIR/$subdomain/")

  def getSortedFDFiles(subdomain:String,id: String) = new File(s"$FDDIR/$subdomain/$id/")
    .listFiles()
    .toIndexedSeq
    .sortBy(f => LocalDate.parse(f.getName.split("\\.")(0),IOService.dateTimeFormatter).toEpochDay)

  def DB_SYNTHESIS_DIR = socrataDir + "/db_synthesis"

  def DECOMPOSTION_DIR = DB_SYNTHESIS_DIR + "/decomposition"

  def FDDIR = DECOMPOSTION_DIR + "/fds/"
  def DECOMPOSITION_EXPORT_CSV_DIR = DECOMPOSTION_DIR + "/csv/"
  def DECOMPOSITION_RESULT_DIR = DECOMPOSTION_DIR + "/results/"
  def DECOMPOSITION_MEASUREMENTS_DIR = DECOMPOSTION_DIR + "/measurements/"

  def dateToStr(date: LocalDate) = IOService.dateTimeFormatter.format(date)

  def getDecompositionCSVExportFile(instance: DatasetInstance,subdomain:String) = {
    val dir = new File(DECOMPOSITION_EXPORT_CSV_DIR + "/" + subdomain + s"/${instance.id}/")
    dir.mkdirs()
    new File(dir.getAbsolutePath + s"/${dateToStr(instance.date)}.csv")
  }

  def getDecompositionResultFiles() = new File(DECOMPOSITION_RESULT_DIR).listFiles().filter(_.getName.endsWith(".json"))

  def socrataDir = IOService.socrataDir
}
