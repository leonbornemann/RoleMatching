package de.hpi.role_matching.cbrm.evidence_based_weighting.isf

import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}

case class ISFMapStorage(mapEntries:IndexedSeq[(ValueTransition[Any],Int)]) extends JsonWritable[ISFMapStorage] {
  val asMap = mapEntries.toMap
}

object ISFMapStorage extends JsonReadable[ISFMapStorage]
