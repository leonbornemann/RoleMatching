package de.hpi.dataset_versioning.data.change.temporal_tables

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables
import de.hpi.dataset_versioning.data.json.helper.TemporalColumnHelper
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

class TemporalColumn(id: String, attrId: Int, lineages: collection.IndexedSeq[EntityFieldLineage]) {

  def writeToStandardFile() = {
    val helper = TemporalColumnHelper(id,attrId,lineages.map(vl => (vl.entityID,vl.lineage.toSerializationHelper)))
    helper.toJsonFile(IOService.getTemporalColumnFile(id,attrId))
  }

}

object TemporalColumn {



  def load(id:String,attrID:Int) = {
    val helper = TemporalColumnHelper.fromJsonFile(IOService.getTemporalColumnFile(id,attrID).getAbsolutePath)
    new TemporalColumn(helper.id,helper.attrId,helper.value.map{case (eID,l) => temporal_tables.EntityFieldLineage(eID,ValueLineage(mutable.TreeMap[LocalDate,Any]() ++ l.lineage))})
  }
}
