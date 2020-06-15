package de.hpi.dataset_versioning.io

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.data.DatasetInstance

object DBSynthesis_IOService {

  def DB_SYNTHESIS_DIR = socrataDir + "/db_synthesis"

  def DECOMPOSTION_DIR = DB_SYNTHESIS_DIR + "/decomposition"

  def DECOMPOSITION_EXPORT_CSV_DIR = DECOMPOSTION_DIR + "/csv/"

  def dateToStr(date: LocalDate) = IOService.dateTimeFormatter.format(date)

  def getDecompositionCSVExportFile(instance: DatasetInstance) = new File(DECOMPOSITION_EXPORT_CSV_DIR + s"${dateToStr(instance.date)}_${instance.id}.csv")


  def socrataDir = IOService.socrataDir


}
