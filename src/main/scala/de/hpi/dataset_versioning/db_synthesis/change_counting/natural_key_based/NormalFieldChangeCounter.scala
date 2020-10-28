package de.hpi.dataset_versioning.db_synthesis.change_counting.natural_key_based

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.AbstractTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

class NormalFieldChangeCounter extends FieldChangeCounter {
  override def countFieldChanges[A](viewInsertTime: LocalDate, f: TemporalFieldTrait[A]): Int = f.getValueLineage.size

  override def countChanges[A](table: AbstractTemporalDatabaseTable[A]): Long = table.dataColumns.flatMap(_.fieldLineages.map(countFieldChanges(null,_))).sum

  override def name: String = "NormalFieldChangeCounter"

  override def countChanges(table: TemporalTable, allDeterminantAttributeIDs: Set[Int]): Long = table.rows.flatMap(_.fields.map(countFieldChanges(null,_))).sum

  override def countColumnChanges[A](tc: TemporalColumnTrait[A], insertTime: LocalDate, colIsPk: Boolean): Long = tc.fieldLineages.map(countFieldChanges(null,_)).sum
}
