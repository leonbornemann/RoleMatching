package de.hpi.dataset_versioning.data.exploration.db_synthesis

import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.io.IOService

object ColnameHistogram extends App {
  IOService.socrataDir = args(0)
  var histories = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
  histories = histories.filter(_.versionsWithChanges.size > 2)
  val md = IOService.getOrLoadCustomMetadataForStandardTimeFrame()
  val colnameToCount = md.metadata
    .values
    .toSeq
    .flatMap(_.columnMetadata.keySet.toSeq)
    .groupBy(identity)
    .mapValues(_.size)
    .toIndexedSeq
    .sortBy(_._2)
  colnameToCount.foreach(t => println(s"${t._1},${t._2}"))
}
