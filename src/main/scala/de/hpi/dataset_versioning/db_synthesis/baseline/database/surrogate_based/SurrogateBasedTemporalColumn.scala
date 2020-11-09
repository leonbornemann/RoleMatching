package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

import scala.collection.mutable.ArrayBuffer

class SurrogateBasedTemporalColumn(nonKeyAttribute: AttributeLineage, rows: ArrayBuffer[SurrogateBasedTemporalRow]) extends TemporalColumnTrait[Any]{

  override def attributeLineage: AttributeLineage = nonKeyAttribute

  override def fieldLineages: collection.IndexedSeq[TemporalFieldTrait[Any]] = rows.map(_.value)

  override def attrID: Int = nonKeyAttribute.attrId
}
