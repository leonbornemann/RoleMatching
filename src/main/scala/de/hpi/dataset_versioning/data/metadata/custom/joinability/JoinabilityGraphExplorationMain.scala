package de.hpi.dataset_versioning.data.metadata.custom.joinability

import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService

object JoinabilityGraphExplorationMain extends App {
  IOService.socrataDir = args(0)
  val startVersion = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val endVersion = LocalDate.parse(args(2),IOService.dateTimeFormatter)
  val explorer = new JoinabilityGraphExplorer()
  explorer.exploreGraphMemory(startVersion,endVersion)
}
