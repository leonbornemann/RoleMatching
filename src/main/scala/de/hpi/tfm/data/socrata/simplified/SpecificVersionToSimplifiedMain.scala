package de.hpi.tfm.data.socrata.simplified

import de.hpi.tfm.io.IOService

import java.time.LocalDate

object SpecificVersionToSimplifiedMain extends App {
  IOService.socrataDir = args(0)
  val id = args(1)
  val version = LocalDate.parse(args(2),IOService.dateTimeFormatter)
  val transformer = new Transformer()
  transformer.transformVersion(id,version)
  println("terminating")
}
