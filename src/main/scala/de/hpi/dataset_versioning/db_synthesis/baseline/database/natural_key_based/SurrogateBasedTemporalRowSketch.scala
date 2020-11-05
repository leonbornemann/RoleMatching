package de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based

import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{AbstractSurrogateBasedTemporalRow}
import de.hpi.dataset_versioning.db_synthesis.sketches.field.{TemporalFieldTrait, Variant2Sketch}

@SerialVersionUID(3L)
class SurrogateBasedTemporalRowSketch(keys: IndexedSeq[Int], val valueSketch: Variant2Sketch, foreignKeys: IndexedSeq[Int])
  extends AbstractSurrogateBasedTemporalRow(keys,valueSketch,foreignKeys) with Serializable{

  override def mergeWithConsistent(keys: IndexedSeq[Int], rightRow: AbstractSurrogateBasedTemporalRow[Int]) = {
    new SurrogateBasedTemporalRowSketch(keys,valueSketch.mergeWithConsistent(rightRow.value).asInstanceOf[Variant2Sketch],IndexedSeq())
  }

  override def cloneWithNewKey(newKEy: Int) = {
    new SurrogateBasedTemporalRowSketch(IndexedSeq(newKEy),valueSketch,foreignKeys)
  }
}
