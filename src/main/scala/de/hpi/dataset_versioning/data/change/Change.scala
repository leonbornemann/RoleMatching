package de.hpi.dataset_versioning.data.change

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.bottom_up.Field

case class Change(t:LocalDate, e:Long, pID:Int, prevValue:Any, newValue:Any) extends JsonWritable[Change]{
  def isDelete: Boolean = prevValue != None && newValue == None

  def isUpdate = prevValue != None && newValue != None

  def isInsert = prevValue == None && newValue != None

  def getValueTuple = (prevValue,newValue)

}

object Change extends JsonReadable[Change]
