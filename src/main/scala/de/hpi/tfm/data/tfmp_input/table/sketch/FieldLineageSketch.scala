package de.hpi.tfm.data.tfmp_input.table.sketch

import de.hpi.tfm.data.tfmp_input.table.AbstractTemporalField

@SerialVersionUID(3L)
trait FieldLineageSketch extends AbstractTemporalField[Int] with Serializable {

  //def valueAt(ts: LocalDate): Int


  def getVariantName: String

  def getBytes: Array[Byte]

}
