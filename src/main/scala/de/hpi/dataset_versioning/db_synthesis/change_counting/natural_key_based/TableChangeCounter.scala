package de.hpi.dataset_versioning.db_synthesis.change_counting.natural_key_based

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait

trait TableChangeCounter {
  def name: String


  def countChanges(table: TemporalTable, allDeterminantAttributeIDs: Set[Int]): Long

  def countChanges(table: TemporalTable): Long = ???

  def countColumnChanges[A](tc: TemporalColumnTrait[A], insertTime: LocalDate, colIsPk: Boolean): Long

}
