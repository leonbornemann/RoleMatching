package de.hpi.dataset_versioning.db_synthesis.top_down.merge.grouping

import de.hpi.dataset_versioning.db_synthesis.top_down.decomposition.normalization.DecomposedTable

class ArityBasedDecomposedTableGrouper() {

  def getGroups(tables: Array[DecomposedTable]): Set[IndexedSeq[DecomposedTable]] = {
    tables.groupBy(_.attributes.size)
      .map{case (k,v) => v.toIndexedSeq}
      .toSet
  }

}
