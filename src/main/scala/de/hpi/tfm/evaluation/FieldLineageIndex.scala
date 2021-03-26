package de.hpi.tfm.evaluation

import de.hpi.tfm.data.socrata.change.temporal_tables.TemporalTable

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
