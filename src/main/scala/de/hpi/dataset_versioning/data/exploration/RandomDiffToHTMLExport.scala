package de.hpi.dataset_versioning.data.exploration

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.data
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.exploration.SingleHTMLExportMain.args
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.io.IOService

import scala.util.Random

object RandomDiffToHTMLExport extends App {
  IOService.socrataDir = args(0)
  val targetDir = args(1)
  val numDsToExport = args(2).toInt
  randomExport()
  //manualExport

  def randomExport() = {
    val mdStartVersion = LocalDate.parse("2019-11-01",IOService.dateTimeFormatter)
    val mdEndVersion = LocalDate.parse("2020-04-30",IOService.dateTimeFormatter)
    val minVersionCount = 2
    IOService.cacheCustomMetadata(mdStartVersion,mdEndVersion)
    val lineages = IOService.readCleanedDatasetLineages()
      .filter(_.versionsWithChanges.size>=minVersionCount)
    val rand = new Random()
    (0 until numDsToExport).foreach(_ => {
      val lineage = lineages(rand.nextInt(lineages.size))
      val (secondVersion,i) = lineage.versionsWithChanges.zipWithIndex.tail(rand.nextInt(lineage.versionsWithChanges.size-1))
      val firstVersion = lineage.versionsWithChanges(i-1)
      val dsBeforeChange = IOService.tryLoadDataset(data.DatasetInstance(lineage.id,firstVersion),true)
      val dsAfterChange = IOService.tryLoadDataset(data.DatasetInstance(lineage.id,secondVersion),true)
      val diff = dsBeforeChange.calculateDataDiff(dsAfterChange)
      val exporter = new DatasetHTMLExporter()
      exporter.exportDiffTableView(dsBeforeChange,dsAfterChange,diff,new File(s"$targetDir/${dsBeforeChange.id}_${dsBeforeChange.version}-->${dsAfterChange.version}.html"))
    })
  }
}
