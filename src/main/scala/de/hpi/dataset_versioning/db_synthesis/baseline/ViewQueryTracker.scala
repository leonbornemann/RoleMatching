package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable

class ViewQueryTracker(ids:IndexedSeq[String]) {
  def updateForSynthTable(newSynthTable: SynthesizedTemporalDatabaseTable, executedMatch: TableUnionMatch[Int]) = {
    newSynthTable.unionedTables.foreach(dttID => {
      val viewID = dttID.viewID
      val toUpdate = byID(viewID)
      toUpdate.update(newSynthTable,executedMatch)
    })
  }


  //init empty mapping
  val byID = ids.map(id => {
    val tt = TemporalTable.load(id)
    (id,new ViewTupleMappingTracker(id,tt.rows.map(_.entityID),tt.attributes.map(_.attrId)))
  }).toMap
}