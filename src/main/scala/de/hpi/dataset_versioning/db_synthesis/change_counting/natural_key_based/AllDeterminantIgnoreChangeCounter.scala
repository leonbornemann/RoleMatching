package de.hpi.dataset_versioning.db_synthesis.change_counting.natural_key_based

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.AbstractTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait

/***
 * Ignores all determinant columns, no matter where they appear (Read: ignores keys AND foreign keys)
 * @param changeCounter
 */
class AllDeterminantIgnoreChangeCounter(changeCounter: FieldChangeCounter) extends TableChangeCounter {

  override def countChanges[A](table: AbstractTemporalDatabaseTable[A]): Long = {
    val insertTime = table.insertTime
    val pk = table.primaryKey.map(_.attrId).toSet
    val cols = table.dataColumns.filter(c => !pk.contains(c.attrID))
    cols.map(c => changeCounter.countColumnChanges(c,insertTime,false)).sum
  }

  override def countChanges(table: TemporalTable, allDeterminantAttributeIDs: Set[Int]): Long = {
    val insertTime = table.insertTime
    table.rows.flatMap(tr => tr.fields
      .zip(table.attributes)
      .withFilter(t => !allDeterminantAttributeIDs.contains(t._2.attrId))
      .map{case (f,_) => changeCounter.countFieldChanges(insertTime,f).toLong}).sum
  }

  override def countColumnChanges[A](tc: TemporalColumnTrait[A], insertTime: LocalDate, colIsPk: Boolean): Long = {
    if(colIsPk) 0
    else changeCounter.countColumnChanges(tc,insertTime,colIsPk)
  }

  override def name: String = changeCounter.name +"_ALL_DETERMINANTS_IGNORED"
}
