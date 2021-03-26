package de.hpi.tfm.data.socrata.json.helper

import de.hpi.tfm.data.socrata.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}

case class TemporalSchemaHelper(val id:String,val attributes:collection.IndexedSeq[AttributeLineageWithHashMap]) extends JsonWritable[TemporalSchemaHelper]

object TemporalSchemaHelper extends JsonReadable[TemporalSchemaHelper]
