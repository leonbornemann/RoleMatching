package de.hpi.dataset_versioning.data.change

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

case class ChangeCube(datasetID:String,
                      colIDTOAttributeMap:mutable.HashMap[Int,mutable.HashMap[LocalDate,Attribute]]=mutable.HashMap(),
                      var inserts:mutable.ArrayBuffer[Change] = mutable.ArrayBuffer[Change](),
                      var deletes:mutable.ArrayBuffer[Change] = mutable.ArrayBuffer[Change](),
                      var updates:mutable.ArrayBuffer[Change] = mutable.ArrayBuffer[Change]()) extends JsonWritable[ChangeCube] with StrictLogging{

  def changeCount(countInitialInserts: Boolean) = {
    val totalNumberChanges = inserts.size + deletes.size + updates.size
    if(countInitialInserts) {
      totalNumberChanges
    }
    else {
      val initialInserts = inserts.groupBy(c => (c.e,c.pID))
        .size
      totalNumberChanges - initialInserts
    }
  }


  def isEmpty: Boolean = inserts.isEmpty && deletes.isEmpty && updates.isEmpty

  def firstTimestamp: Option[LocalDate] = {
    if(isEmpty) None
    else{
      val minI = if(inserts.isEmpty) LocalDate.MAX else inserts.minBy(_.t.toEpochDay).t
      val minD = if(deletes.isEmpty) LocalDate.MAX else deletes.minBy(_.t.toEpochDay).t
      val minU = if(updates.isEmpty) LocalDate.MAX else updates.minBy(_.t.toEpochDay).t
      Some(Seq(minI,minD,minU).minBy(_.toEpochDay))
    }
  }

  def filterChangesInPlace(filterFunction: Change => Boolean) = {
    inserts = inserts.filter(filterFunction)
    deletes = deletes.filter(filterFunction)
    updates = updates.filter(filterFunction)
    this
  }

  def addAll(other: ChangeCube) = {
    inserts ++= other.inserts
    deletes ++= other.deletes
    updates ++= other.updates
    other.colIDTOAttributeMap.foreach{case (cID,map) => {
      val myMap = colIDTOAttributeMap.getOrElseUpdate(cID,mutable.HashMap[LocalDate,Attribute]())
      myMap.addAll(map)
    }}
    this
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

  def load(id:String) = ChangeCube.fromJsonFile(IOService.getChangeFile(id))

  def loadAllChanges(ids: Seq[String]) = {
    val changeCubes = mutable.ArrayBuffer[ChangeCube]()
    var count = 0
    ids.foreach(id => {
      logger.debug(s"Loading changes for $id")
      changeCubes += ChangeCube.fromJsonFile(IOService.getChangeFile(id))
      count+=1
      logger.debug(s"Loaded $count/${ids.size} changes")
    })
    changeCubes
  }

}
