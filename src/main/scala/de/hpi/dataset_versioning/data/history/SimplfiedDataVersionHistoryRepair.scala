package de.hpi.dataset_versioning.data.history

import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.io.IOService

import java.io.File

object SimplfiedDataVersionHistoryRepair extends App {
  IOService.socrataDir = args(0)
  val list = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
  list.foreach(h => {
    val versions = h.versionsWithChanges
    versions.foreach(v => {
      val curFile = IOService.getSimplifiedDatasetFile(DatasetInstance(h.id, v))
      val simplfiedFileExists = new File(curFile).exists()
      if(!simplfiedFileExists){
        println(h.id + " " + v.format(IOService.dateTimeFormatter))
      }

    })
  })

}
