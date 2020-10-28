package de.hpi.dataset_versioning.db_synthesis.change_counting.natural_key_based

import java.time.LocalDate

import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

trait FieldChangeCounter extends TableChangeCounter {

  def countFieldChanges[A](viewInsertTime: LocalDate, f: TemporalFieldTrait[A]): Int

}
