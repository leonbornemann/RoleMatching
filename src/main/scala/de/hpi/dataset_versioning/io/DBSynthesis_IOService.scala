package de.hpi.dataset_versioning.io

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.data.DatasetInstance

object DBSynthesis_IOService {
  def getDiscoveredFDFiles(id: String) = new File(FDDIR)
    .listFiles()
    .filter(_.getName.contains(id))

  def DB_SYNTHESIS_DIR = socrataDir + "/db_synthesis"

  def DECOMPOSTION_DIR = DB_SYNTHESIS_DIR + "/decomposition"

  def FDDIR = DECOMPOSTION_DIR + "/fds/"
  def DECOMPOSITION_EXPORT_CSV_DIR = DECOMPOSTION_DIR + "/csv/"
  def DECOMPOSITION_RESULT_DIR = DECOMPOSTION_DIR + "/results/"

  def dateToStr(date: LocalDate) = IOService.dateTimeFormatter.format(date)

  def getDecompositionCSVExportFile(instance: DatasetInstance,subdomain:String) = {
    val dir = new File(DECOMPOSITION_EXPORT_CSV_DIR + "/" + subdomain + s"/${instance.id}/")
    dir.mkdirs()
    new File(dir.getAbsolutePath + s"/${dateToStr(instance.date)}.csv")
  }

  def getDecompositionResultFiles() = new File(DECOMPOSITION_RESULT_DIR).listFiles().filter(_.getName.endsWith(".json"))

  def socrataDir = IOService.socrataDir
}
