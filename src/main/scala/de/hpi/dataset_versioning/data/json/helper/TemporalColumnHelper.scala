package de.hpi.dataset_versioning.data.json.helper

import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineageWithHashMap

case class TemporalColumnHelper(id: String, attributeLineage: AttributeLineageWithHashMap, value: collection.IndexedSeq[(Long,ValueLineageWithHashMap)]) extends JsonWritable[TemporalColumnHelper] {

}
object TemporalColumnHelper extends JsonReadable[TemporalColumnHelper]
