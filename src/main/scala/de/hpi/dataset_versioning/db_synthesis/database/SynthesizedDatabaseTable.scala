package de.hpi.dataset_versioning.db_synthesis.database

import java.time.LocalDate

import de.hpi.dataset_versioning.data.simplified.{Attribute, RelationalDataset}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTable

import scala.collection.mutable

class SynthesizedDatabaseTable(val table:RelationalDataset, var unionedTables:collection.mutable.ArrayBuffer[TableUnionInfo], val attributes:collection.IndexedSeq[Attribute]) {

  def addUnionTable(info: TableUnionInfo) = {
    unionedTables += info
  }

}
object SynthesizedDatabaseTable {

  def initFromSingleDecomposedTable(id:String,version:LocalDate,t:DecomposedTable) = {
    val attrMapping = t.attributes.zip(t.attributes)
      .toMap //identity mapping
    new SynthesizedDatabaseTable(RelationalDataset.createEmpty(id,version),mutable.ArrayBuffer(TableUnionInfo(t,attrMapping,0,0)),t.attributes)
  }
}