package de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch, SurrogateBasedTemporalRow}
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

class UpdateChangeCounter() {

  def countFieldChanges(r: SurrogateBasedTemporalRow) = {
    r.value.numValues-1//.getValueLineage.size.toLong TODO: make this more efficient? - create numEntries method
  }

  def countFieldChanges[A](tuple: collection.Seq[TemporalFieldTrait[A]]) = {
    assert(tuple.size==1)
    tuple(0).numValues-1
  }

  def countChanges(table:SurrogateBasedSynthesizedTemporalDatabaseTableAssociation) = {
    table.surrogateBasedTemporalRows.map(r => countFieldChanges(r)).sum
  }

  def countChanges(table:SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch) = {
    table.surrogateBasedTemporalRowSketches.map(r => countFieldChanges(Seq(r.valueSketch))).sum
  }

  def countChanges(table:TemporalTable) = {
    table.rows.flatMap(_.fields.map(vl => vl.lineage.size.toLong -1)).sum
  }

}
