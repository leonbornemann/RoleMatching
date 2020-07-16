package de.hpi.dataset_versioning.db_synthesis.database

import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization.DecomposedTable

case class TableUnionInfo(t:DecomposedTable,columnMapping:Map[Attribute,Attribute],cost:Int,benefit:Int) {

}
