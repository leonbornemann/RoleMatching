package de.hpi.dataset_versioning.db_synthesis.baseline.config

object GLOBAL_CONFIG {
  val COUNT_SURROGATE_INSERTS: Boolean = true
  val CHANGE_COUNT_METHOD = new DatasetInsertIgnoreFieldChangeCounter()
}
