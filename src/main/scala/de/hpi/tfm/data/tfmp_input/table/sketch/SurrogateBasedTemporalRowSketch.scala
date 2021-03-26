package de.hpi.tfm.data.tfmp_input.table.sketch

import de.hpi.tfm.data.tfmp_input.table.AbstractSurrogateBasedTemporalRow

@SerialVersionUID(3L)
class SurrogateBasedTemporalRowSketch(keys: IndexedSeq[Int], val valueSketch: FactLineageSketch, foreignKeys: IndexedSeq[Int])
  extends AbstractSurrogateBasedTemporalRow(keys,valueSketch,foreignKeys) with Serializable{

  override def mergeWithConsistent(keys: IndexedSeq[Int], rightRow: AbstractSurrogateBasedTemporalRow[Int]) = {
    new SurrogateBasedTemporalRowSketch(keys,valueSketch.mergeWithConsistent(rightRow.value).asInstanceOf[FactLineageSketch],IndexedSeq())
  }

  override def cloneWithNewKey(newKEy: Int) = {
    new SurrogateBasedTemporalRowSketch(IndexedSeq(newKEy),valueSketch,foreignKeys)
  }
}
