package de.hpi.dataset_versioning.db_synthesis.sketches.field

@SerialVersionUID(3L)
trait FieldLineageSketch extends AbstractTemporalField[Int] with Serializable {

  //def valueAt(ts: LocalDate): Int


  def getVariantName: String

  def getBytes: Array[Byte]

}
