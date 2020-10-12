package de.hpi.dataset_versioning.db_synthesis.baseline.config
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.AbstractTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

class DatasetAndRowInitialInsertIgnoreFieldChangeCounter extends FieldChangeCounter {

  override def countFieldChanges[A](viewInsertTime: LocalDate, f: TemporalFieldTrait[A]): Int = {
    val it = f.getValueLineage
      .iteratorFrom(viewInsertTime.plusDays(1))
    var (_,curVal) = it.next()
    while(it.hasNext && (f.isWildcard(curVal) || f.isRowDelete(curVal)))
      curVal = it.next()._2
    it.size
  }

  override def name: String = "DatasetAndRowInsertIgnoreFieldChangeCounter"

  override def countChanges(table: TemporalTable, primaryKeyAttributeIDs: Set[Int]): Long = {
    val insertTime = table.insertTime
    table.rows.flatMap(tr => tr.fields.map(f => countFieldChanges(insertTime,f).toLong)).sum
  }

  override def countColumnChanges[A](tc: TemporalColumnTrait[A], insertTime: LocalDate, colIsPk: Boolean): Long = {
    tc.fieldLineages.map(f => countFieldChanges(insertTime,f).toLong).sum
  }

  override def countChanges[A](table: AbstractTemporalDatabaseTable[A]): Long = {
    val insertTime = table.insertTime
    table.columns.map(c => countColumnChanges(c,insertTime,false)).sum
  }
}
