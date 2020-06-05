package de.hpi.dataset_versioning.data.metadata.custom.joinability

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService

object JoinabilityGraphSizeReductionMain extends App {
  IOService.socrataDir = args(0)
  val explorer = new JoinabilityGraphExplorer()
  val startVersion = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val endVersion = LocalDate.parse(args(2),IOService.dateTimeFormatter)
  explorer.transformToSmallRepresentation(startVersion,endVersion,new File(args(3)))
  //explorer.explore(path)

}
