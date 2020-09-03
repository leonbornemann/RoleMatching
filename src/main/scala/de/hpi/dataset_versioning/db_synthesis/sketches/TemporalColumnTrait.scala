package de.hpi.dataset_versioning.db_synthesis.sketches

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage

trait TemporalColumnTrait[T] {
  def attributeLineage :AttributeLineage

  def fieldLineages:collection.IndexedSeq[TemporalFieldTrait[T]]

  def attrID :Int

}
