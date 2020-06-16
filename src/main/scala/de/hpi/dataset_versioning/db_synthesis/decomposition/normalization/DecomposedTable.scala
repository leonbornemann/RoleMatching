package de.hpi.dataset_versioning.db_synthesis.decomposition.normalization

import java.time.LocalDate

import de.hpi.dataset_versioning.data.JsonWritable
import de.hpi.dataset_versioning.data.simplified.Attribute

case class DecomposedTable(originalID:String,version:LocalDate,id:Int, attributes:collection.IndexedSeq[Attribute],primaryKey:collection.Set[String],foreignKeys:collection.Set[String]) extends JsonWritable[DecomposedTable] {

}
