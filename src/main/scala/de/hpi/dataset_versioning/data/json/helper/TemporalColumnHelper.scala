package de.hpi.dataset_versioning.data.json.helper

import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineageWithHashMap
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

case class TemporalColumnHelper(id: String, attributeLineage: AttributeLineageWithHashMap, value: collection.IndexedSeq[(Long,ValueLineageWithHashMap)]) extends JsonWritable[TemporalColumnHelper] {

}
object TemporalColumnHelper extends JsonReadable[TemporalColumnHelper]
