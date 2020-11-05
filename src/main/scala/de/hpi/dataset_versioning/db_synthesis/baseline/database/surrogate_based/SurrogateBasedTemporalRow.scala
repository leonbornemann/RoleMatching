package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.SurrogateBasedTemporalRowSketch
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.field.{TemporalFieldTrait, Variant2Sketch}

@SerialVersionUID(3L)
class SurrogateBasedTemporalRow(keys:IndexedSeq[Int],val valueLineage:ValueLineage,foreignKeys:IndexedSeq[Int]) extends AbstractSurrogateBasedTemporalRow[Any](keys,valueLineage,foreignKeys) with Serializable{
  def toRowSketch = new SurrogateBasedTemporalRowSketch(keys,Variant2Sketch.fromValueLineage(valueLineage),foreignKeys)

  override def mergeWithConsistent(keys: IndexedSeq[Int], rightRow: AbstractSurrogateBasedTemporalRow[Any]) = {
    //val a:TemporalFieldTrait[Any] = rightRow.value
    val mergedValueLineage = valueLineage.mergeWithConsistent(rightRow.value).asInstanceOf[ValueLineage]
    new SurrogateBasedTemporalRow(keys,mergedValueLineage,IndexedSeq())
  }

  override def cloneWithNewKey(newKey: Int) = {
    new SurrogateBasedTemporalRow(IndexedSeq(newKey),valueLineage,foreignKeys)
  }
}
