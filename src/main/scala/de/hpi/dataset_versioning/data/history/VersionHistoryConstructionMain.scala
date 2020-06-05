package de.hpi.dataset_versioning.data.history

import de.hpi.dataset_versioning.io.IOService

object VersionHistoryConstructionMain extends App {
  IOService.socrataDir = args(0)
  val versionHistoryConstruction = new VersionHistoryConstruction()
  versionHistoryConstruction.constructVersionHistory()
}
