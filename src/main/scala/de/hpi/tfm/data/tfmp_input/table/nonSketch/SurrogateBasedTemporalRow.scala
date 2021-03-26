package de.hpi.tfm.data.tfmp_input.table.nonSketch

import de.hpi.tfm.data.tfmp_input.table.AbstractSurrogateBasedTemporalRow
import de.hpi.tfm.data.tfmp_input.table.sketch.{FactLineageSketch, SurrogateBasedTemporalRowSketch}

@SerialVersionUID(3L)
class SurrogateBasedTemporalRow(pk:IndexedSeq[Int], val valueLineage:FactLineage, foreignKeys:IndexedSeq[Int]) extends AbstractSurrogateBasedTemporalRow[Any](pk,valueLineage,foreignKeys) with Serializable{
  def toRowSketch = new SurrogateBasedTemporalRowSketch(pk,FactLineageSketch.fromValueLineage(valueLineage),foreignKeys)

  override def mergeWithConsistent(keys: IndexedSeq[Int], rightRow: AbstractSurrogateBasedTemporalRow[Any]) = {
    //val a:TemporalFieldTrait[Any] = rightRow.value
    val mergedValueLineage = valueLineage.mergeWithConsistent(rightRow.value).asInstanceOf[FactLineage]
    new SurrogateBasedTemporalRow(keys,mergedValueLineage,IndexedSeq())
  }

  override def cloneWithNewKey(newKey: Int) = {
    new SurrogateBasedTemporalRow(IndexedSeq(newKey),valueLineage,foreignKeys)
  }
}
