package de.hpi.socrata.json.helper

import de.hpi.socrata.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.socrata.{JsonReadable, JsonWritable}

case class TemporalSchemaHelper(val id:String,val attributes:collection.IndexedSeq[AttributeLineageWithHashMap]) extends JsonWritable[TemporalSchemaHelper]

object TemporalSchemaHelper extends JsonReadable[TemporalSchemaHelper]
