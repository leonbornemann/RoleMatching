package de.hpi.tfm.data.socrata.metadata.custom.schemaHistory

import de.hpi.tfm.data.socrata.change.temporal_tables.TemporalTable
import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, AttributeState}
import de.hpi.tfm.data.socrata.json.helper.TemporalSchemaHelper
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.io.IOService

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
  def fromTemporalTable(table: TemporalTable) = TemporalSchema(table.id,table.attributes)


  def load(id:String) = {
    val file = IOService.getTemporalSchemaFile(id).getAbsolutePath
    val helper = TemporalSchemaHelper.fromJsonFile(file)
    TemporalSchema(helper.id,helper.attributes.map(al => new AttributeLineage(al.attrId,collection.mutable.TreeMap[LocalDate,AttributeState]() ++ al.lineage)))
  }

  def readFromTemporalTable(id:String) = {
    TemporalSchema(id,TemporalTable.load(id).attributes)
  }

}
