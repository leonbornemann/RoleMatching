package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.sketches.table.{DecomposedTemporalTableSketch, SynthesizedTemporalDatabaseTableSketch}
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.oneshot.ScriptMain.{args, subDomainInfo, subdomain}

import scala.sys.process._
import scala.language.postfixOps

object ScriptMain2 extends App {


  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subdomainIds = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq

  val id = "fg6s-gzvg"
  val bcnfID = 0
  val aID = 1
  val tt = TemporalTable.load(id)
  val changes = ChangeCube.load(id)
  val pIDs = changes.allChanges.groupBy(_.pID).keySet
  val ttSchema = tt.attributes.map(_.attrId).toSet
  val weird = tt.rows.filter(_.fields.exists(_.getValueLineage.size==0))
  assert(ttSchema==pIDs)
  val associations = DecomposedTemporalTable.loadAllAssociations(subdomain,id) ++ DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
  associations.foreach(dtt => tt.project(dtt).projection.writeTableSketch(dtt.primaryKey.map(_.attrId)))

  for (id <- ids){
    println(id)

  }
}
