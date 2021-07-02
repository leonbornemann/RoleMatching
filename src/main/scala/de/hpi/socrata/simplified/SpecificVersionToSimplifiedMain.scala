package de.hpi.socrata.simplified

import de.hpi.socrata.io.Socrata_IOService

import java.time.LocalDate

object SpecificVersionToSimplifiedMain extends App {
  Socrata_IOService.socrataDir = args(0)
  val id = args(1)
  val version = LocalDate.parse(args(2),Socrata_IOService.dateTimeFormatter)
  val transformer = new Transformer()
  transformer.transformVersion(id,version)
  println("terminating")
}
