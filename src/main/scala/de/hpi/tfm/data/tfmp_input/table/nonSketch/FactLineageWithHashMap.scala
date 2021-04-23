package de.hpi.tfm.data.tfmp_input.table.nonSketch

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}

import java.time.LocalDate

case class FactLineageWithHashMap(lineage: Map[LocalDate, Any]) extends JsonWritable[FactLineageWithHashMap]{
  def toFactLineage = FactLineage.fromSerializationHelper(this)

}

object FactLineageWithHashMap extends JsonReadable[FactLineageWithHashMap]
