package de.hpi.socrata.tfmp_input.table.nonSketch

import de.hpi.socrata.{JsonReadable, JsonWritable}

import java.time.LocalDate

case class FactLineageWithHashMap(lineage: Map[LocalDate, Any]) extends JsonWritable[FactLineageWithHashMap]{
  def toFactLineage = FactLineage.fromSerializationHelper(this)

}

object FactLineageWithHashMap extends JsonReadable[FactLineageWithHashMap]
