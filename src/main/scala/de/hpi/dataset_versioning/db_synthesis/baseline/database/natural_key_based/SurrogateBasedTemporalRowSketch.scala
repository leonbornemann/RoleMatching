package de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based

import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.AbstractSurrogateBasedTemporalRow
import de.hpi.dataset_versioning.db_synthesis.sketches.field.{TemporalFieldTrait, Variant2Sketch}

@SerialVersionUID(3L)
class SurrogateBasedTemporalRowSketch(keys: IndexedSeq[Int], val valueSketch: Variant2Sketch, foreignKeys: IndexedSeq[Int])
  extends AbstractSurrogateBasedTemporalRow(keys,valueSketch,foreignKeys) with Serializable{

}
