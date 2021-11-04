package de.hpi.role_matching.cbrm.evidence_based_weighting.isf

import de.hpi.role_matching.cbrm.data.ValueTransition
import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}

case class ISFMapStorage(mapEntries:IndexedSeq[(ValueTransition,Int)]) extends JsonWritable[ISFMapStorage] {
  val asMap = mapEntries.toMap
}

object ISFMapStorage extends JsonReadable[ISFMapStorage]
