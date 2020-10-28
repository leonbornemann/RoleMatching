package de.hpi.dataset_versioning.db_synthesis.database.query_tracking

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.SynthesizedTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch

class ViewQueryTracker(ids:collection.IndexedSeq[String]) {
  def updateForSynthTable(newSynthTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, executedMatch: TableUnionMatch[Int]) = {
    newSynthTable.getUnionedTables.foreach(dttID => {
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