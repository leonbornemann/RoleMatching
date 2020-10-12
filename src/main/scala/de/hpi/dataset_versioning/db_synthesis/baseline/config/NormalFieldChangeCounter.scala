package de.hpi.dataset_versioning.db_synthesis.baseline.config
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.AbstractTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

class NormalFieldChangeCounter extends FieldChangeCounter {
  override def countFieldChanges[A](viewInsertTime: LocalDate, f: TemporalFieldTrait[A]): Int = f.getValueLineage.size

  override def countChanges[A](table: AbstractTemporalDatabaseTable[A]): Long = table.columns.flatMap(_.fieldLineages.map(countFieldChanges(null,_))).sum

  override def name: String = "NormalFieldChangeCounter"

  override def countChanges(table: TemporalTable, primaryKeyAttributeIDs: Set[Int]): Long = table.rows.flatMap(_.fields.map(countFieldChanges(null,_))).sum

  override def countColumnChanges[A](tc: TemporalColumnTrait[A], insertTime: LocalDate, colIsPk: Boolean): Long = tc.fieldLineages.map(countFieldChanges(null,_)).sum
}
