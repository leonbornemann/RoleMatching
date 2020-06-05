package de.hpi.dataset_versioning.data.diff.semantic

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

class DataDiffCalculator() extends StrictLogging{

  def aggregateAllDataDiffs(checkPointInterval:Int = 7) = {
    val snapshots = IOService.getSortedDatalakeVersions()
    val toCompare = snapshots.zipWithIndex
      .filter(t => t._2 % checkPointInterval==0 && IOService.compressedSnapshotExists(t._1))
      .map(_._1)
    var totalSummary = new DatalakeDiffSummary()
    logger.trace("Calculating Summary for {}",toCompare)
    for( i <- 1 until toCompare.size){
      val curSummary = calculateDataDiff(toCompare(i-1),toCompare(i))
      totalSummary.plus(curSummary)
      IOService.clearUncompressedSnapshot(toCompare(i-1))
    }
    logger.trace(s"Total Summary of changes between datalake snapshots in time period ${toCompare.head}-${toCompare.last} with checkpoint interval of $checkPointInterval days")
    totalSummary.print()
    //free up memory:
    toCompare.takeRight(2).foreach(IOService.clearUncompressedSnapshot(_))
  }


  def calculateDataDiff(previous: LocalDate, current: LocalDate) = {
    val matching = GroundTruthDatasetMatching.getGroundTruthMatching(previous,current)
    val datalakeDiffSummary = new DatalakeDiffSummary()
    matching.matchings.foreach{ case (prev,cur) => {
      try {
        val previousLoadedDataset = IOService.loadDataset(prev)
        val currentLoadedDataset = IOService.loadDataset(cur)
        val dataDiff = previousLoadedDataset.calculateDataDiff(currentLoadedDataset)
        if(!dataDiff.incomplete) {
          if (!dataDiff.inserts.isEmpty) datalakeDiffSummary.numInserts += 1
          if (!dataDiff.deletes.isEmpty) datalakeDiffSummary.numDeletes += 1
          if (!dataDiff.updates.isEmpty) datalakeDiffSummary.numTupleChanges += 1
        }
        if(dataDiff.schemaChange.projection.isDefined) datalakeDiffSummary.numProjections +=1
        if(dataDiff.schemaChange.columnInsert.isDefined) datalakeDiffSummary.numColumnInserts +=1
        if(dataDiff.incomplete) datalakeDiffSummary.numDiffIncomplete +=1
        if(!dataDiff.inserts.isEmpty || !dataDiff.deletes.isEmpty || !dataDiff.updates.isEmpty) datalakeDiffSummary.numDataChanges +=1
      } catch {
        case _:Throwable => logger.debug(s"Exception while trying to parse either $prev or $cur")
      }
      datalakeDiffSummary.processed +=1
      if(datalakeDiffSummary.processed%1000==0){
        datalakeDiffSummary.print()
      }
    }}
    logger.debug(s"Done")
    datalakeDiffSummary.print()
    datalakeDiffSummary
  }

}
