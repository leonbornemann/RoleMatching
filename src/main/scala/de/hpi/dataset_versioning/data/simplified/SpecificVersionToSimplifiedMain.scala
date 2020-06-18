package de.hpi.dataset_versioning.data.simplified

import java.time.LocalDate

import de.hpi.dataset_versioning.data.LocalDateSerializer
import de.hpi.dataset_versioning.io.IOService

object SpecificVersionToSimplifiedMain extends App {
  IOService.socrataDir = args(0)
  val id = args(1)
  val version = LocalDate.parse(args(2),IOService.dateTimeFormatter)
  val transformer = new Transformer()
  transformer.transformVersion(id,version)
  println("terminating")
}
