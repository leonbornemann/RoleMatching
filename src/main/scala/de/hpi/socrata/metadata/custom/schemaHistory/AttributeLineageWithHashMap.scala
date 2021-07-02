package de.hpi.socrata.metadata.custom.schemaHistory

import de.hpi.socrata.change.temporal_tables.attribute.{AttributeLineage, AttributeState}

import java.time.LocalDate
import scala.collection.mutable

case class AttributeLineageWithHashMap(val attrId:Int,val lineage:Map[LocalDate,AttributeState]) {
  def toAttributeLineage: AttributeLineage = {
    new AttributeLineage(attrId,mutable.TreeMap[LocalDate,AttributeState]() ++ lineage)
  }

  def toDecomposedTemporalTable: AttributeLineage = new AttributeLineage(attrId,mutable.TreeMap[LocalDate,AttributeState]() ++ lineage)

}

object AttributeLineageWithHashMap {
  def from(al:AttributeLineage) = AttributeLineageWithHashMap(al.attrId,al.lineage.toMap)
}
