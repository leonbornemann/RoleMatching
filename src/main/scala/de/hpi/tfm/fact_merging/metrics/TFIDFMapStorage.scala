package de.hpi.tfm.fact_merging.metrics

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition

case class TFIDFMapStorage(mapEntries:IndexedSeq[(ValueTransition[Any],Int)]) extends JsonWritable[TFIDFMapStorage] {
  val asMap = mapEntries.toMap
}

object TFIDFMapStorage extends JsonReadable[TFIDFMapStorage]
