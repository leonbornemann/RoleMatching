package de.hpi.dataset_versioning.oneshot.statisticis

import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.exploration.db_synthesis.ConditionalProbabilityExplorationMain.histories
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.io.IOService

object ChangeHistogramBySubdomain extends App {

  IOService.socrataDir = args(0)
  var histories = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
  val idToVersions = histories.map(h => (h.id,h))
    .toMap
  val md = IOService.getOrLoadCustomMetadataForStandardTimeFrame()
  var byDomain = idToVersions
    .groupBy{case (k,list) => {
      val allVersions = list.versionsWithChanges
      val firstVersion = DatasetInstance(k, allVersions.head)
      if(!md.metadata.contains(firstVersion))
        null
      else
        md.metadata(firstVersion).topLevelDomain
    }}
  byDomain.foreach{case (k,map) => {
    map.foreach(entry => println(s"$k,${entry._2.versionsWithChanges.size}"))
  }}
}
