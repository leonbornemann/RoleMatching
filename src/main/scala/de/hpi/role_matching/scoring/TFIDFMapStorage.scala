package de.hpi.role_matching.scoring

import de.hpi.socrata.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.socrata.{JsonReadable, JsonWritable}

case class TFIDFMapStorage(mapEntries:IndexedSeq[(ValueTransition[Any],Int)]) extends JsonWritable[TFIDFMapStorage] {
  val asMap = mapEntries.toMap
}

object TFIDFMapStorage extends JsonReadable[TFIDFMapStorage]
