package de.hpi.dataset_versioning.data.metadata.custom.schemaHistory

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.change.{AttributeLineage, AttributeState, TemporalTable}
import de.hpi.dataset_versioning.io.IOService

case class TemporalSchema(val id:String,val attributes:collection.IndexedSeq[AttributeLineage]) extends JsonWritable[TemporalSchema] {

  def writeToStandardFile() = {
    val file = IOService.getTemporalSchemaFile(id)
    TemporalSchemaHelper(id,attributes.map(al => AttributeLineageWithHashMap(al.attrId,al.lineage.toMap)))
      .toJsonFile(file)
  }

}


object TemporalSchema extends JsonReadable[TemporalSchema]{

  def load(id:String) = {
    val file = IOService.getTemporalSchemaFile(id).getAbsolutePath
    val helper = TemporalSchemaHelper.fromJsonFile(file)
    TemporalSchema(helper.id,helper.attributes.map(al => new AttributeLineage(al.attrId,collection.mutable.TreeMap[LocalDate,AttributeState]() ++ al.lineage)))
  }

  def readFromTemporalTable(id:String) = {
    TemporalSchema(id,TemporalTable.load(id).attributes)
  }

}
