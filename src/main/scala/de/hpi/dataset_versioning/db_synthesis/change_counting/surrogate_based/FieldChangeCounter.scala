package de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based

import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

import java.time.LocalDate

trait FieldChangeCounter extends TableChangeCounter {

  def countFieldChanges[A](f: TemporalFieldTrait[A]): (Int,Int)

}
