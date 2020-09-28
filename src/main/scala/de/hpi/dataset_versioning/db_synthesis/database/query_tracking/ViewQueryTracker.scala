package de.hpi.dataset_versioning.db_synthesis.database.query_tracking

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.SynthesizedTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch

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