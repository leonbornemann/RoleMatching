package de.hpi.dataset_versioning.db_synthesis.sketches.column

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

trait TemporalColumnTrait[T] {
  def getBagOfWords(): collection.Map[T,Int] = fieldLineages
    .flatMap(fls => fls.nonWildCardValues)
    .groupBy(identity)
    .map{case (k,v) => (k,v.size)}

  def attributeLineage: AttributeLineage

  def fieldLineages: collection.IndexedSeq[TemporalFieldTrait[T]]

  def attrID: Int

}
