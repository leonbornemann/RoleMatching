package de.hpi.dataset_versioning.db_synthesis.baseline.config

import de.hpi.dataset_versioning.db_synthesis.change_counting.natural_key_based.DatasetInsertIgnoreFieldChangeCounter
import de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based.CountAllChangeCounter

object GLOBAL_CONFIG {

  var INDEX_DEPTH = 2

  val COUNT_SURROGATE_INSERTS: Boolean = false
  //val CHANGE_COUNT_METHOD = new DatasetInsertIgnoreFieldChangeCounter()
  val NEW_CHANGE_COUNT_METHOD = new CountAllChangeCounter()
}
