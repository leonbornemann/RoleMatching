package de.hpi.dataset_versioning.data.exploration

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.data
import de.hpi.dataset_versioning.io.IOService

object DIffPairExportTest extends App {
  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/fromServer"

  val exporter = new DatasetHTMLExporter()
  val targetDir = new File("/home/leon/Desktop/")

  val aID = "test-0001"
  val bID = "test-0002"
  val v1 = LocalDate.parse("2019-11-01",IOService.dateTimeFormatter)
  val v2 = LocalDate.parse("2019-11-02",IOService.dateTimeFormatter)
  val dsABeforeChange = IOService.loadSimplifiedRelationalDataset(data.DatasetInstance(aID, v1))
  val dsAAfterChange = IOService.loadSimplifiedRelationalDataset(data.DatasetInstance(aID, v2))
  val dsBBeforeChange = IOService.loadSimplifiedRelationalDataset(data.DatasetInstance(bID, v1))
  val dsBAfterChange = IOService.loadSimplifiedRelationalDataset(data.DatasetInstance(bID, v2))
  val diffA = dsABeforeChange.calculateDataDiff(dsAAfterChange)
  val diffB = dsBBeforeChange.calculateDataDiff(dsBAfterChange)
  val similarity = diffB.calculateDiffSimilarity(diffA)
  val schemaSimilarityThreshold = 0.001
  val newValueSimilarityThreshold = 0.2
  val deletedValueSimilarityThreshold = 0.4
  val fieldUpdateSimilarityThreshold = 0.05
  val correlationInfo = ChangeCorrelationInfo("asd",aID, bID,2,2, 2, 1.0, 1.0,1.0)
  //DiffSimilarity(schemaSimilarity:Double,newValueSimilarity:Double,deletedValueSimilarity:Double,fieldUpdateSimilarity:Double) {
  if(similarity.schemaSimilarity>schemaSimilarityThreshold || similarity.newValueSimilarity>newValueSimilarityThreshold || similarity.deletedValueSimilarity > deletedValueSimilarityThreshold || similarity.fieldUpdateSimilarity > fieldUpdateSimilarityThreshold){
    exporter.exportDiffPairToTableView(dsABeforeChange, dsAAfterChange, diffA,
      dsBBeforeChange, dsBAfterChange, diffB,
      correlationInfo,
      similarity,
      new File(s"$targetDir/${aID}_AND_${bID}_$v2.html"))
    true
  }
}
