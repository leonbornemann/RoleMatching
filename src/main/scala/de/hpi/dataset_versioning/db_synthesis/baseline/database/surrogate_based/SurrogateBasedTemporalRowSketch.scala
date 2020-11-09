package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import de.hpi.dataset_versioning.db_synthesis.sketches.field.Variant2Sketch

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
