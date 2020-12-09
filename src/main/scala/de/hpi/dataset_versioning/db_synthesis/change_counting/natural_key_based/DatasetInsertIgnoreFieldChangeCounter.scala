package de.hpi.dataset_versioning.db_synthesis.change_counting.natural_key_based

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

class DatasetInsertIgnoreFieldChangeCounter() extends FieldChangeCounter{

  override def countFieldChanges[A](viewInsertTime: LocalDate, f: TemporalFieldTrait[A]) = {
    f.getValueLineage.iteratorFrom(viewInsertTime.plusDays(1)).toSet.filter(!ValueLineage.isWildcard(_)).size
  }

  override def countChanges(table: TemporalTable, allDeterminantAttributeIDs: Set[Int]): Long = {
    val insertTime = table.insertTime
    table.rows.flatMap(tr => tr.fields.map(f => countFieldChanges(insertTime,f).toLong)).sum
  }

  override def countColumnChanges[A](tc: TemporalColumnTrait[A],insertTime:LocalDate, colIsPk: Boolean): Long = {
    tc.fieldLineages.map(f => countFieldChanges(insertTime,f).toLong).sum
  }

  override def name: String = "DatasetInsertIgnoreFieldChangeCounter"
}
