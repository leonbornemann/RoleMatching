package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.data.change.{ChangeCube, ChangeExporter}
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.sketches.table.{DecomposedTemporalTableSketch, SynthesizedTemporalDatabaseTableSketch}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import scala.sys.process._
import scala.language.postfixOps

object ScriptMain2 extends App {


  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  val idsToIgnore = "v6vf-nfxy,wrvz-psew,cygx-ui4j,ijzp-q8t2,x2n5-8w5q,68nd-jvt3,4aki-r3np,sxs8-h27x".split(",").toSet
  val ids = DecomposedTemporalTable.filterNotFullyDecomposedTables(subdomain,subdomainIds)
    .filter(!idsToIgnore.contains(_))

  val exporter = new ChangeExporter

  val id = "mex4-ppfc"
  var tt = TemporalTable.load(id)
  var weird = tt.rows.filter(_.fields.exists(_.getValueLineage.size==0))
  exporter.exportAllChanges(id)
  tt = TemporalTable.load(id)
  weird = tt.rows.filter(_.fields.exists(_.getValueLineage.size==0))
  val bcnfID = 0
  val aID = 1
  val changes = ChangeCube.load(id)
  val pIDs = changes.allChanges.groupBy(_.pID).keySet
  val ttSchema = tt.attributes.map(_.attrId).toSet
  assert(ttSchema==pIDs)
  val associations = DecomposedTemporalTable.loadAllAssociations(subdomain,id) ++ DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
  associations.foreach(dtt => tt.project(dtt).projection.writeTableSketch(dtt.primaryKey.map(_.attrId)))

  println(s"processing ${ids.size} ids")
  for (id <- ids){
    println(id)
    exporter.exportAllChanges(id)
    val tt = TemporalTable.load(id)
    val changes = ChangeCube.load(id)
    val ttSchema = tt.attributes.map(_.attrId).toSet
    val weird = tt.rows.filter(_.fields.exists(_.getValueLineage.size==0))
    var associations = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
    if(DBSynthesis_IOService.decomposedTemporalAssociationsExist(subdomain,id))
      associations = associations ++ DecomposedTemporalTable.loadAllAssociations(subdomain,id)
    associations.foreach(dtt => tt.project(dtt).projection.writeTableSketch(dtt.primaryKey.map(_.attrId)))
  }
}
