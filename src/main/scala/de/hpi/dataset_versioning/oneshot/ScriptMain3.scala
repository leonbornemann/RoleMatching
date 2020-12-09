package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTable, TemporalTableDecomposer}
import de.hpi.dataset_versioning.db_synthesis.database.table.BCNFTableSchema
import de.hpi.dataset_versioning.io.IOService

object ScriptMain3 extends App {
  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/fromServer/"
  private val subdomain = "org.cityofchicago"
  val schemata = BCNFTableSchema.loadAllBCNFTableSchemata(subdomain)
  val empty = schemata.filter(_.attributes.size==0)
  println()

  val versionHistories = DatasetVersionHistory.load()
    .map(h => (h.id,h)).toMap
  val empties = empty.map(_.id.viewID).map(id => {
    val versionHistory = versionHistories(id)
    val temporalSchema = TemporalSchema.load(id)
    val attrLineageByID = temporalSchema.attributes
      .map(al => (al.attrId,al))
      .toMap
    val latestVersion = versionHistory.latestChangeTimestamp
    val decomposedTablesAtLastTimestamp = DecomposedTable.load(subdomain,id,latestVersion)
    decomposedTablesAtLastTimestamp.filter(_.attributes.isEmpty).size
  }).sum
  println(empties)
  println()
  val id = "72uz-ikdv" //72uz-ikdv.6(SK0,SK1,SK3,SK6  ,)
  val decomposer = new TemporalTableDecomposer(subdomain,id,versionHistories(id))
  decomposer.createDecomposedTemporalTables()


}
