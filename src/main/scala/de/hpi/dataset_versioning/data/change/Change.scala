package de.hpi.dataset_versioning.data.change

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.bottom_up.Field

case class Change(t:LocalDate, e:Long, pID:Int, prevValue:Any, newValue:Any) extends JsonWritable[Change]{
  def isDelete: Boolean = prevValue != ReservedChangeValues.NOT_EXISTANT && newValue == ReservedChangeValues.NOT_EXISTANT

  def isUpdate = prevValue != ReservedChangeValues.NOT_EXISTANT && newValue != ReservedChangeValues.NOT_EXISTANT

  def isInsert = prevValue == ReservedChangeValues.NOT_EXISTANT && newValue != ReservedChangeValues.NOT_EXISTANT

  def getValueTuple = (prevValue,newValue)

}

object Change extends JsonReadable[Change]
