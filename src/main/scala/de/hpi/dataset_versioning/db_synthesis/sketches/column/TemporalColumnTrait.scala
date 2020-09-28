package de.hpi.dataset_versioning.db_synthesis.sketches.column

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

trait TemporalColumnTrait[T] {
  def attributeLineage: AttributeLineage

  def fieldLineages: collection.IndexedSeq[TemporalFieldTrait[T]]

  def attrID: Int

}
