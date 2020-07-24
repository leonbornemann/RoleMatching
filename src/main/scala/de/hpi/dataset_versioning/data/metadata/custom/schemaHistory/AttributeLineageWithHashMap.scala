package de.hpi.dataset_versioning.data.metadata.custom.schemaHistory

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.AttributeState

import scala.collection.mutable

case class AttributeLineageWithHashMap(val attrId:Int,val lineage:Map[LocalDate,AttributeState]) {

}
