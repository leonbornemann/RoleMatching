package de.hpi.dataset_versioning.data.change

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.simplified.Attribute

case class Change(t:LocalDate, e:Long, pID:Int, value:Any) extends JsonWritable[Change]{
  def isDelete: Boolean = value == ReservedChangeValues.NOT_EXISTANT_ROW || value == ReservedChangeValues.NOT_EXISTANT_COL || value == ReservedChangeValues.NOT_EXISTANT_DATASET
}

object Change extends JsonReadable[Change]
