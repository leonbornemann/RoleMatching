package de.hpi.dataset_versioning.db_synthesis.bottom_up

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

import scala.collection.mutable

case class ValueLineageWithHashMap(lineage: Map[LocalDate, Any]) extends JsonWritable[ValueLineageWithHashMap]{

}

object ValueLineageWithHashMap extends JsonReadable[ValueLineageWithHashMap]
