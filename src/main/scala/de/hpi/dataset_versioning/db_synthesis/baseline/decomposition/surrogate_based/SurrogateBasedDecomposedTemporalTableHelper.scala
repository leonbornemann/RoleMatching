package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based

import de.hpi.dataset_versioning.data.change.temporal_tables.SurrogateAttributeLineage
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

import scala.collection.mutable.ArrayBuffer

case class SurrogateBasedDecomposedTemporalTableHelper(id: DecomposedTemporalTableIdentifier,
                                                       surrogateKey: IndexedSeq[SurrogateAttributeLineage],
                                                       attributes: ArrayBuffer[AttributeLineageWithHashMap],
                                                       foreignKeyToReferredTables: IndexedSeq[(SurrogateAttributeLineage, collection.IndexedSeq[DecomposedTemporalTableIdentifier])]) extends JsonWritable[SurrogateBasedDecomposedTemporalTableHelper]{
  def toSurrogateBasedDecomposedTemporalTable = {
    new SurrogateBasedDecomposedTemporalTable(id,
      surrogateKey,
      attributes.map(_.toDecomposedTemporalTable),
      foreignKeyToReferredTables
    )
  }

}
object SurrogateBasedDecomposedTemporalTableHelper extends JsonReadable[SurrogateBasedDecomposedTemporalTableHelper]
