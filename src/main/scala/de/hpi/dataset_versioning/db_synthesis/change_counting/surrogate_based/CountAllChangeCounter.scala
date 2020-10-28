package de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow}
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

class CountAllChangeCounter {

  def countFieldChanges(r: SurrogateBasedTemporalRow) = {
    r.value.numValues//.getValueLineage.size.toLong TODO: make this more efficient? - create numEntries method
  }

  def countFieldChanges[A](tuple: collection.Seq[TemporalFieldTrait[A]]) = {
    assert(tuple.size==1)
    tuple(0).numValues
  }

  def countChanges(table:SurrogateBasedSynthesizedTemporalDatabaseTableAssociation) = {
    table.rows.map(r => countFieldChanges(r)).sum
  }

  def countChanges(table:TemporalTable) = {
    table.rows.flatMap(_.fields.map(vl => vl.lineage.size.toLong)).sum
  }
}
