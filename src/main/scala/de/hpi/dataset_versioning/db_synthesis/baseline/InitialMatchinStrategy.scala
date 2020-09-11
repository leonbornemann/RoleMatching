package de.hpi.dataset_versioning.db_synthesis.baseline

object InitialMatchinStrategy extends Enumeration {
  type InitialMatchinStrategy = Value
  val INDEX_BASED,NAIVE_PAIRWISE = Value
}