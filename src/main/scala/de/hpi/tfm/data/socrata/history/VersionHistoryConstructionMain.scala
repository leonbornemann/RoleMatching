package de.hpi.tfm.data.socrata.history

import de.hpi.tfm.io.IOService

object VersionHistoryConstructionMain extends App {
  IOService.socrataDir = args(0)
  val versionHistoryConstruction = new VersionHistoryConstruction()
  versionHistoryConstruction.constructVersionHistory()
}
