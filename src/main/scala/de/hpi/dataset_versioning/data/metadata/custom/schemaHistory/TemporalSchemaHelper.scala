package de.hpi.dataset_versioning.data.metadata.custom.schemaHistory

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

case class TemporalSchemaHelper(val id:String,val attributes:collection.IndexedSeq[AttributeLineageWithHashMap]) extends JsonWritable[TemporalSchemaHelper]

object TemporalSchemaHelper extends JsonReadable[TemporalSchemaHelper]
