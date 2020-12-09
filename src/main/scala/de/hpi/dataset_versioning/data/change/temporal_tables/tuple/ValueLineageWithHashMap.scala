package de.hpi.dataset_versioning.data.change.temporal_tables.tuple

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

import java.time.LocalDate

case class ValueLineageWithHashMap(lineage: Map[LocalDate, Any]) extends JsonWritable[ValueLineageWithHashMap]{

}

object ValueLineageWithHashMap extends JsonReadable[ValueLineageWithHashMap]
