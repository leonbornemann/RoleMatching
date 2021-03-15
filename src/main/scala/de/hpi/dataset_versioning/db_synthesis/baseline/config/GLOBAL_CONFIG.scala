package de.hpi.dataset_versioning.db_synthesis.baseline.config

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference
import de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based.UpdateChangeCounter
import de.hpi.dataset_versioning.db_synthesis.sketches.field.{AbstractTemporalField, TemporalFieldTrait}

object GLOBAL_CONFIG {

  def OPTIMIZATION_TARGET_FUNCTION[A](tr1: TupleReference[A], tr2: TupleReference[A]) = AbstractTemporalField.MUTUAL_INFORMATION(tr1,tr2)

  var ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS = false

  //val CHANGE_COUNT_METHOD = new DatasetInsertIgnoreFieldChangeCounter()
  val CHANGE_COUNT_METHOD = new UpdateChangeCounter()
}
