package de.hpi.dataset_versioning.db_synthesis.top_down_no_change.merge

import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.database.SynthesizedDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization.DecomposedTable

class ChangeInconsistencyFinder() {

  def mergeHasInconsistencies(t1: DecomposedTable, t2: DecomposedTable, mapping: Map[Attribute, Attribute]):Boolean = {
    ??? //TODO!
  }

  def mergeHasInconsistencies(synthTable: SynthesizedDatabaseTable, t2: DecomposedTable, mapping: Map[Attribute, Attribute]):Boolean = {
    ??? //TODO!
  }

}
