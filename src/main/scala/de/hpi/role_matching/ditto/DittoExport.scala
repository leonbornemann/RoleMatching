package de.hpi.role_matching.ditto

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate

object DittoExport extends App {
  val datasource = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(datasource)
  private val vertexSetFile = args(1)
  val trainTimeEnd = LocalDate.parse(args(2))
  val resultFile = new File(args(3))
  val vertices = Roleset.fromJsonFile(vertexSetFile)
  val exporter = new DittoExporter(vertices,trainTimeEnd,resultFile)
}
