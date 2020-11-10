package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.db_synthesis.database.table.BCNFTableSchema
import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

object ScriptMain3 extends App {
  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/fromServer/"
  val schemata = BCNFTableSchema.loadAllBCNFTableSchemata("org.cityofchicago")
  val empty = schemata.filter(_.attributes.size==0)
  println()

}
