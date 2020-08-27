package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage

trait TemporalDatabaseTableTrait {
  def primaryKey:collection.Set[AttributeLineage]

  def nonKeyAttributeLineages:collection.IndexedSeq[AttributeLineage]

}
