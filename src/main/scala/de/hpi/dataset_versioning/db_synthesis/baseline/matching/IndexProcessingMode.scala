package de.hpi.dataset_versioning.db_synthesis.baseline.matching

object IndexProcessingMode extends Enumeration {
  type IndexProcessingMode = Value
  val EXECUTE_MATCH,SERIALIZE_EDGE_CANDIDATE = Value
}