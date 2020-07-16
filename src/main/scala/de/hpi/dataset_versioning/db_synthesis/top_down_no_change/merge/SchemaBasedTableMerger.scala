package de.hpi.dataset_versioning.db_synthesis.top_down_no_change.merge

import de.hpi.dataset_versioning.db_synthesis.database.SynthesizedDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization.DecomposedTable
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.merge.measures.TableMergeMeasure

class SchemaBasedTableMerger(benefitMeasure:TableMergeMeasure, costMeasure:TableMergeMeasure, allowChangeInconsistencies:Boolean) {

  val changeInconsistencyFinder = new ChangeInconsistencyFinder()

  def tryTableMerge(t1: DecomposedTable, t2: DecomposedTable) = {
    if(t1.attributes.map(_.name).toSet == t2.attributes.map(_.name).toSet){
      val mapping = t1.attributes.sortBy(_.name).zip(t2.attributes.sortBy(_.name))
        .toMap
      val hasInconsisitencies = if(allowChangeInconsistencies) false else changeInconsistencyFinder.mergeHasInconsistencies(t1,t2,mapping)
      if(!hasInconsisitencies || allowChangeInconsistencies) {
        val benefit = benefitMeasure.calculate(t1, t2, mapping)
        val cost = costMeasure.calculate(t1, t2, mapping)
        Some(TableMergeResult(mapping, benefit, cost)) //TODO do we need to store something e}xtra here? such as field mapping functions/ tuple merges?
      } else None
    } else{
      None
    }
  }

  def tryTableMerge(synthTable: SynthesizedDatabaseTable, t: DecomposedTable) = {
    if(synthTable.attributes.map(_.name).toSet == t.attributes.map(_.name).toSet){
      val mapping = synthTable.attributes.sortBy(_.name).zip(t.attributes.sortBy(_.name))
        .toMap
      val hasInconsisitencies = if(allowChangeInconsistencies) false else changeInconsistencyFinder.mergeHasInconsistencies(synthTable,t,mapping)
      if(!hasInconsisitencies || allowChangeInconsistencies) {
        val benefit = benefitMeasure.calculate(synthTable, t, mapping)
        val cost = costMeasure.calculate(synthTable, t, mapping)
        Some(TableMergeResult(mapping, benefit, cost)) //TODO do we need to store something e}xtra here? such as field mapping functions/ tuple merges?
      } else None
    } else{
      None
    }
  }


}
