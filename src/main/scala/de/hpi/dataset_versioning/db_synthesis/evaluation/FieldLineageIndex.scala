package de.hpi.dataset_versioning.db_synthesis.evaluation

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.data.change.{Change, ChangeCube}

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class FieldLineageIndex(value: Set[TemporalTable]) {

  def getFieldLineage(tableID: String, attrID: Int, entityID: Int) = {
    index(tableID)((entityID,attrID))
  }

  def toMap(tt: TemporalTable) = tt.rows.flatMap(tr => {
      tr.fields.zipWithIndex.map{case (vl,attrPos) => ((tr.entityID,tt.attributes(attrPos).attrId),vl)}
    }).toMap

  val index = value
    .map(tt => (tt.id,toMap(tt)))
    .toMap
}
