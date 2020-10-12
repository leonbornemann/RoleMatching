package de.hpi.dataset_versioning.db_synthesis.baseline.config

import java.time.LocalDate
import java.time.temporal.TemporalField

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.AbstractTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

class DatasetInsertIgnoreFieldChangeCounter() extends FieldChangeCounter{

  override def countFieldChanges[A](viewInsertTime: LocalDate, f: TemporalFieldTrait[A]) = {
    f.getValueLineage.iteratorFrom(viewInsertTime.plusDays(1)).toSet.filter(!ValueLineage.isWildcard(_)).size
  }

  override def countChanges[A](table: AbstractTemporalDatabaseTable[A]): Long = {
    val insertTime = table.insertTime
    table.columns.map(c => countColumnChanges(c,insertTime,false)).sum
  }

  override def countChanges(table: TemporalTable, primaryKeyAttributeIDs: Set[Int]): Long = {
    val insertTime = table.insertTime
    table.rows.flatMap(tr => tr.fields.map(f => countFieldChanges(insertTime,f).toLong)).sum
  }

  override def countColumnChanges[A](tc: TemporalColumnTrait[A],insertTime:LocalDate, colIsPk: Boolean): Long = {
    tc.fieldLineages.map(f => countFieldChanges(insertTime,f).toLong).sum
  }

  override def name: String = "DatasetInsertIgnoreFieldChangeCounter"
}
