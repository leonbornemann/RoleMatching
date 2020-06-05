package de.hpi.dataset_versioning.data.exploration

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService

object SingleHTMLExportMain extends App {
  IOService.socrataDir = args(0)
  val dsStrID = args(1)
  val dsVersion = LocalDate.parse(args(2),IOService.dateTimeFormatter)
  val targetDir = args(3)
  val ds = IOService.tryLoadDataset(dsStrID,dsVersion)
  val exporter = new DatasetHTMLExporter()
  exporter.toHTML(ds,new File(s"$targetDir/${ds.id}_${ds.version}.html"))
}
