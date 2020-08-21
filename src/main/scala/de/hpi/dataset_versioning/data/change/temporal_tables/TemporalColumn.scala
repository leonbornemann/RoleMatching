package de.hpi.dataset_versioning.data.change.temporal_tables

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables
import de.hpi.dataset_versioning.data.json.helper.TemporalColumnHelper
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

class TemporalColumn(val id: String, val attributeLineage:AttributeLineage, val lineages: collection.IndexedSeq[EntityFieldLineage]) {

  def attrId = attributeLineage.attrId

  def writeToStandardFile() = {
    val attrLineage = AttributeLineageWithHashMap.from(attributeLineage)
    val helper = TemporalColumnHelper(id,attrLineage,lineages.map(vl => (vl.entityID,vl.lineage.toSerializationHelper)))
    helper.toJsonFile(IOService.getTemporalColumnFile(id,attrId))
  }

}

object TemporalColumn {

  def load(id:String,attrID:Int) = {
    val helper = TemporalColumnHelper.fromJsonFile(IOService.getTemporalColumnFile(id,attrID).getAbsolutePath)
    new TemporalColumn(helper.id,helper.attributeLineage.toAttributeLineage,helper.value.map{case (eID,l) => temporal_tables.EntityFieldLineage(eID,ValueLineage(mutable.TreeMap[LocalDate,Any]() ++ l.lineage))})
  }
}
