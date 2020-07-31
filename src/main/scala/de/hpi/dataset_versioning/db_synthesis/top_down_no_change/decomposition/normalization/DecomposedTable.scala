package de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

case class DecomposedTable(originalID:String,version:LocalDate,id:Int, attributes:collection.IndexedSeq[Attribute],primaryKey:collection.Set[Attribute],foreignKeys:collection.Set[Attribute]) extends JsonWritable[DecomposedTable] {
  def sortedPrimaryKeyColIDs = primaryKey.map(_.id).toIndexedSeq.sorted

  def compositeID = originalID + s".$id"

}

object DecomposedTable extends JsonReadable[DecomposedTable] {
  def load(subdomain: String, id: String, date: LocalDate) =
    fromJsonObjectPerLineFile(DBSynthesis_IOService.getDecomposedTableFile(subdomain,id,date).getAbsolutePath)
}
