package de.hpi.dataset_versioning.data.change

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

import java.time.LocalDate

case class Change(t:LocalDate, e:Long, pID:Int, value:Any) extends JsonWritable[Change]{
  def isDelete: Boolean = value == ReservedChangeValues.NOT_EXISTANT_ROW || value == ReservedChangeValues.NOT_EXISTANT_COL || value == ReservedChangeValues.NOT_EXISTANT_DATASET
}

object Change extends JsonReadable[Change]
