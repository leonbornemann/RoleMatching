package de.hpi.dataset_versioning.db_synthesis.baseline.config

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.AbstractTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

trait FieldChangeCounter extends TableChangeCounter {

  def countFieldChanges[A](viewInsertTime: LocalDate, f: TemporalFieldTrait[A]):Int

}
