package de.hpi.socrata.tfmp_input.table

@SerialVersionUID(3L)
abstract class AbstractSurrogateBasedTemporalRow[T](val keys: IndexedSeq[Int], val value: TemporalFieldTrait[T],val foreignKeys: IndexedSeq[Int]) extends Serializable{
  def mergeWithConsistent(keys: IndexedSeq[Int], rightRow: AbstractSurrogateBasedTemporalRow[T]):AbstractSurrogateBasedTemporalRow[T]

  def cloneWithNewKey(curSurrogateKeyCounter: Int):AbstractSurrogateBasedTemporalRow[T]

}
