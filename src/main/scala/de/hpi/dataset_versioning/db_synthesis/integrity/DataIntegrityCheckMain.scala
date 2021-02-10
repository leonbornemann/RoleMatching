package de.hpi.dataset_versioning.db_synthesis.integrity

import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch}
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.IOService

object DataIntegrityCheckMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val schemata = AssociationSchema.loadAllAssociationsInSubdomain(subdomain)
  val byTableAndAttrID = schemata.groupBy(as => (as.id.viewID,as.attributeLineage.attrId))
  val filtered = byTableAndAttrID.filter(_._2.size>1)
  filtered.foreach{case (k,v) => {
    println("------------------------------------------------")
    println(s"${k} appears in all:")
    v.map(_.id).foreach(println)
    println("------------------------------------------------")
  }}
  assert(filtered.isEmpty)
  //assert that for each association schema we find input:
  schemata.foreach(as => {
    assert(SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.getStandardOptimizationInputFile(as.id).exists())
    assert(SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.getStandardOptimizationInputFile(as.id).exists())
  })
  //for all inputs assert that they are in the set of association schemata!
  //SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.getStandardOptimizationInputFile()
}
