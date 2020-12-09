package de.hpi.dataset_versioning.data.change.temporal_tables

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.{EntityFieldLineage, ValueLineage}
import de.hpi.dataset_versioning.data.json.helper.TemporalColumnHelper
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.io.IOService

import java.time.LocalDate
import scala.collection.mutable

class TemporalColumn(val id: String,
                     val attributeLineage:AttributeLineage,
                     val lineages: collection.IndexedSeq[EntityFieldLineage]) extends TemporalColumnTrait[Any]{
  def cardinality = {
    lineages.map(_.lineage.getValueLineage).toSet.size
  }


  def attrId = attributeLineage.attrId

  def writeToStandardFile() = {
    val attrLineage = AttributeLineageWithHashMap.from(attributeLineage)
    val helper = TemporalColumnHelper(id,attrLineage,lineages.map(vl => (vl.entityID,vl.lineage.toSerializationHelper)))
    helper.toJsonFile(IOService.getTemporalColumnFile(id,attrId))
  }

  override def fieldLineages = lineages.map(_.lineage)

  override def attrID: Int = attributeLineage.attrId
}

object TemporalColumn {

  def load(id:String,attrID:Int) = {
    val helper = TemporalColumnHelper.fromJsonFile(IOService.getTemporalColumnFile(id,attrID).getAbsolutePath)
    new TemporalColumn(helper.id,helper.attributeLineage.toAttributeLineage,helper.value.map{case (eID,l) => EntityFieldLineage(eID,ValueLineage(mutable.TreeMap[LocalDate,Any]() ++ l.lineage))})
  }
}
