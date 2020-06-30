package de.hpi.dataset_versioning.db_synthesis.top_down.merge

import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.top_down.decomposition.normalization.DecomposedTable

class ChangeInconsistencyFinder() {
  def mergeHasInconsistencies(t1: DecomposedTable, t2: DecomposedTable, mapping: Map[Attribute, Attribute]):Boolean = {
    ??? //TODO!
  }

}
