package de.hpi.dataset_versioning.data.metadata.custom.schemaHistory

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, AttributeState}
import de.hpi.dataset_versioning.data.json.helper.TemporalSchemaHelper
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.IOService

import java.time.LocalDate

case class TemporalSchema(val id:String,val attributes:collection.IndexedSeq[AttributeLineage]) extends JsonWritable[TemporalSchema] {

  def valueAt(version: LocalDate) = attributes.map(al => al.valueAt(version))

  def byID = attributes.map(al => (al.attrId,al))
    .toMap


  def nameToAttributeState(version:LocalDate) = {
    attributes
      .map(al => al.valueAt(version)._2)
      .filter(!_.isNE)
      .map(as => (as.attr.get.name,as.attr.get))
      .toMap
  }

  def writeToStandardFile() = {
    val file = IOService.getTemporalSchemaFile(id)
    TemporalSchemaHelper(id,attributes.map(al => AttributeLineageWithHashMap.from(al)))
      .toJsonFile(file)
  }

  def lastTimestamp = attributes.maxBy(_.lineage.keySet.max).lineage.keySet.max

}


object TemporalSchema extends JsonReadable[TemporalSchema]{
  def fromTemporalTable(table: TemporalTable) = TemporalSchema(table.attributes)


  def load(id:String) = {
    val file = IOService.getTemporalSchemaFile(id).getAbsolutePath
    val helper = TemporalSchemaHelper.fromJsonFile(file)
    TemporalSchema(helper.id,helper.attributes.map(al => new AttributeLineage(al.attrId,collection.mutable.TreeMap[LocalDate,AttributeState]() ++ al.lineage)))
  }

  def readFromTemporalTable(id:String) = {
    TemporalSchema(id,TemporalTable.load(id).attributes)
  }

}
