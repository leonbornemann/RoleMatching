package de.hpi.dataset_versioning.db_synthesis.baseline.config

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference
import de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based.UpdateChangeCounter
import de.hpi.dataset_versioning.db_synthesis.sketches.field.{AbstractTemporalField, TemporalFieldTrait}

object GLOBAL_CONFIG {
  def OPTIMIZATION_TARGET_FUNCTION[A](tr1: TupleReference[A], tr2: TupleReference[A]) = AbstractTemporalField.ENTROPY_REDUCTION(tr1,tr2)

  val SINGLE_LAYER_INDEX: Boolean = true


  var INDEX_DEPTH = 2

  val COUNT_SURROGATE_INSERTS: Boolean = false
  //val CHANGE_COUNT_METHOD = new DatasetInsertIgnoreFieldChangeCounter()
  val NEW_CHANGE_COUNT_METHOD = new UpdateChangeCounter()
}
