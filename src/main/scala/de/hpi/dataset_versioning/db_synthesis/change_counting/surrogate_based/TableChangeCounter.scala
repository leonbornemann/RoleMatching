package de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait

import java.time.LocalDate

trait TableChangeCounter {
  def name: String

  def countChanges(table: TemporalTable): (Int,Int) = ???

  def countChanges[A](table: TemporalDatabaseTableTrait[A]): (Int,Int) = ???

  def countColumnChanges[A](tc: TemporalColumnTrait[A]): (Int,Int)

}
