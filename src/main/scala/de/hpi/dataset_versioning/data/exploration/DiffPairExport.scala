package de.hpi.dataset_versioning.data.exploration

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.exploration.RandomDiffToHTMLExport.args
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.io.Source
import scala.util.Random

object DiffPairExport extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val sourceFile = args(1)
  val targetDir = args(2)
  val md = IOService.getOrLoadCustomMetadataForStandardTimeFrame()
  val perPairCount = 3
  val sampleSize = 100
  var skippedBecauseTooLowSimilarityCount = 0
  var exportedTotal =0
  val exporter = new DatasetHTMLExporter()
  exportDiffPairs

  private def exportDiffPairs = {
    val lineages = IOService.readCleanedDatasetLineages
      .map(l => (l.id,l))
      .toMap
    val datasetPairs = Source.fromFile(sourceFile)
      .getLines()
      .toSeq
      .tail
      .map(l => {
        val tokens = l.split(",")
        //c("A","B","P(A)","P(B)","P(A AND B)","P(A|B)","P(B|A)")
        ChangeCorrelationInfo(tokens(0),tokens(1),tokens(2),tokens(3).toInt,tokens(4).toInt,tokens(5).toInt,tokens(6).toDouble,tokens(7).toDouble,tokens(8).toDouble)
      })
    val allCorrelationsByDataaset = mutable.HashMap[String,mutable.ArrayBuffer[ChangeCorrelationInfo]]()
    datasetPairs.foreach(dp => {
      allCorrelationsByDataaset.getOrElseUpdate(dp.idA,mutable.ArrayBuffer()) += dp
      allCorrelationsByDataaset.getOrElseUpdate(dp.idB,mutable.ArrayBuffer()) += dp
    })
    val datasetSample = Random.shuffle(allCorrelationsByDataaset.keySet.toIndexedSeq)//.take(sampleSize)
        .iterator
    //go through all datasets
    while(datasetSample.hasNext && exportedTotal < sampleSize*perPairCount){
      val curDataset = datasetSample.next()
      logger.debug(s"Starting export for $curDataset")
      val correlated = allCorrelationsByDataaset(curDataset)
      val byCorrelatedDataset = correlated.map(cp => (if(cp.idA==curDataset) cp.idB else cp.idA,cp))
        .iterator
      //go through datasets that correlated with this one
      var exported = 0
      while(byCorrelatedDataset.hasNext && exported<perPairCount){
        val (curCorrellatedDataset,correlationInfo) = byCorrelatedDataset.next()
        val lA = lineages(curDataset)
        val lB = lineages(curCorrellatedDataset)
        val sharedChangeTimestamps = Random.shuffle(lA.versionsWithChanges.tail.intersect(lB.versionsWithChanges.tail)) //Seq(lA.versionsWithChanges.tail.intersect(lB.versionsWithChanges.tail).head)//
        //go through all shared timestamps:
        var i=0
        while(i< perPairCount && i<sharedChangeTimestamps.size){
          val curDiffTimestamp = sharedChangeTimestamps(i)
          val wasInteresting = checkForInterestingDiff(correlationInfo, lA, lB, curDiffTimestamp)
          if(wasInteresting) {
            exported+=1
            exportedTotal +=1
          } else{
            skippedBecauseTooLowSimilarityCount +=1
          }
          i+=1
          logger.trace(s"Exported $exportedTotal out of ${exportedTotal+skippedBecauseTooLowSimilarityCount} datasets (${100*exportedTotal / (exportedTotal+skippedBecauseTooLowSimilarityCount).toDouble}%)")
        }
        IOService.cachedMetadata.clear()
      }
    }
  }

  private def checkForInterestingDiff(correlationInfo: ChangeCorrelationInfo, lA: DatasetVersionHistory, lB: DatasetVersionHistory, curDiffTimestamp: LocalDate):Boolean = {
    val prevVersionA = lA.versionsWithChanges.takeWhile(_.isBefore(curDiffTimestamp)).last
    val prevVersionB = lB.versionsWithChanges.takeWhile(_.isBefore(curDiffTimestamp)).last
    val dsABeforeChange = IOService.loadSimplifiedRelationalDataset(data.DatasetInstance(lA.id, prevVersionA))
    val dsAAfterChange = IOService.loadSimplifiedRelationalDataset(data.DatasetInstance(lA.id, curDiffTimestamp))
    val dsBBeforeChange = IOService.loadSimplifiedRelationalDataset(data.DatasetInstance(lB.id, prevVersionB))
    val dsBAfterChange = IOService.loadSimplifiedRelationalDataset(data.DatasetInstance(lB.id, curDiffTimestamp))
    val diffA = dsABeforeChange.calculateDataDiff(dsAAfterChange)
    val diffB = dsBBeforeChange.calculateDataDiff(dsBAfterChange)
    val similarity = diffB.calculateDiffSimilarity(diffA)
    val schemaSimilarityThreshold = 0.001
    val newValueSimilarityThreshold = 0.2
    val deletedValueSimilarityThreshold = 0.4
    val fieldUpdateSimilarityThreshold = 0.05
    //DiffSimilarity(schemaSimilarity:Double,newValueSimilarity:Double,deletedValueSimilarity:Double,fieldUpdateSimilarity:Double) {
    if(similarity.schemaSimilarity>schemaSimilarityThreshold || similarity.newValueSimilarity>newValueSimilarityThreshold || similarity.deletedValueSimilarity > deletedValueSimilarityThreshold || similarity.fieldUpdateSimilarity > fieldUpdateSimilarityThreshold){
      exporter.exportDiffPairToTableView(dsABeforeChange, dsAAfterChange, diffA,
        dsBBeforeChange, dsBAfterChange, diffB,
        correlationInfo,
        similarity,
        new File(s"$targetDir/${lA.id}_AND_${lB.id}_$curDiffTimestamp.html"))
      true
    } else{
      false
    }

  }
}

case class ChangeCorrelationInfo(domain:String,idA: String, idB: String,P_A: Int,P_B: Int, P_A_AND_B: Int, P_A_IF_B: Double, P_B_IF_A: Double,significance:Double)
