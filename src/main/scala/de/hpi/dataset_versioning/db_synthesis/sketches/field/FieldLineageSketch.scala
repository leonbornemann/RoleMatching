package de.hpi.dataset_versioning.db_synthesis.sketches.field

import java.time.LocalDate

@SerialVersionUID(3L)
trait FieldLineageSketch extends AbstractTemporalField[Int] with Serializable {
  def valueAt(ts: LocalDate): Int


  def getVariantName: String

  def getBytes: Array[Byte]

}
