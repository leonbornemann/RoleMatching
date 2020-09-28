package de.hpi.dataset_versioning.db_synthesis.database

import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTable

case class TableUnionInfo(t:DecomposedTable,columnMapping:Map[Attribute,Attribute],cost:Int,benefit:Int) {

}
