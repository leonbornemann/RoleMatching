package de.hpi.dataset_versioning.data.change

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

case class ChangeCube(datasetID:String,
                      colIDTOAttributeMap:mutable.HashMap[Int,mutable.HashMap[LocalDate,Attribute]]=mutable.HashMap(),
                      inserts:mutable.ArrayBuffer[Change] = mutable.ArrayBuffer[Change](),
                      deletes:mutable.ArrayBuffer[Change] = mutable.ArrayBuffer[Change](),
                      updates:mutable.ArrayBuffer[Change] = mutable.ArrayBuffer[Change]()) extends JsonWritable[ChangeCube] with StrictLogging{

  def addAll(other: ChangeCube) = {
    inserts ++= other.inserts
    deletes ++= other.deletes
    updates ++= other.updates
    other.colIDTOAttributeMap.foreach{case (cID,map) => {
      val myMap = colIDTOAttributeMap.getOrElseUpdate(cID,mutable.HashMap[LocalDate,Attribute]())
      myMap.addAll(map)
    }}
  }


  def addToAttributeNameMapping(v:LocalDate,attributes:collection.Iterable[Attribute]) ={
    attributes.foreach(a => {
      colIDTOAttributeMap.getOrElseUpdate(a.id,mutable.HashMap[LocalDate,Attribute]()).put(v,a)
    })
  }

  def addChanges(changes: Seq[Change]) = {
    changes.foreach(c => {
      if(c.isInsert) inserts+=c
      else if(c.isDelete) deletes+=c
      else if(c.isUpdate) updates+=c
      else throw new AssertionError("uncaught case for change type")
    })
  }

  def allChanges = inserts ++ deletes ++ updates

}

object ChangeCube extends JsonReadable[ChangeCube] with StrictLogging {
  def loadAllChanges(ids: Seq[String]) = {
//    val changeCubes = mutable.ArrayBuffer[ChangeCube]()
//    ids.foreach(id => {
//      val cube = ChangeCube(id,)
//      cube.datasetID = Some(id)
//      logger.debug(s"Loading changes for $id")
//      val changes = Change.fromJsonObjectPerLineFile(IOService.getChangeFile(id))
//      cube.addChanges(changes)
//      changeCubes +=cube
//    })
//    changeCubes
  }

}
