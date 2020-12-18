package de.hpi.dataset_versioning.db_synthesis.baseline.config

import de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based.{UpdateChangeCounter, Wildcard0_5Counter}

object GLOBAL_CONFIG {
  val SINGLE_LAYER_INDEX: Boolean = true


  var INDEX_DEPTH = 2

  val COUNT_SURROGATE_INSERTS: Boolean = false
  //val CHANGE_COUNT_METHOD = new DatasetInsertIgnoreFieldChangeCounter()
  val NEW_CHANGE_COUNT_METHOD = new UpdateChangeCounter()
}
