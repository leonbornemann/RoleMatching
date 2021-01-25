package de.hpi.dataset_versioning.data.change

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.IOService

import java.time.LocalDate
import scala.collection.mutable

case class ChangeCube(datasetID:String,
                      colIDTOAttributeMap:mutable.HashMap[Int,mutable.HashMap[LocalDate,Attribute]]=mutable.HashMap(),
                      var allChanges:mutable.ArrayBuffer[Change] = mutable.ArrayBuffer[Change]()) extends JsonWritable[ChangeCube] with StrictLogging{
  def addChange(change: Change) = {
    allChanges +=change

  }


  def changeCount(countInitialInserts: Boolean):Int = {
    if(countInitialInserts) allChanges.size
    else {
      allChanges.groupBy(_.e)
        .map(t => t._2.size -1)
        .reduce((a,b) => a+b)
    }
  }

  def toTemporalTable() = {
    TemporalTable.from(this)
  }

  def isEmpty: Boolean = allChanges.isEmpty

  def firstTimestamp: Option[LocalDate] = {
    if(isEmpty) None
    else{
      val min = if(allChanges.isEmpty) LocalDate.MAX else allChanges.minBy(_.t.toEpochDay).t
      Some(min)
    }
  }

  def filterChangesInPlace(filterFunction: Change => Boolean) = {
    allChanges = allChanges.filter(filterFunction)
    this
  }

  def addAll(other: ChangeCube) = {
    allChanges ++= other.allChanges
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

  def addChanges(changes: collection.Iterable[Change]) = {
    allChanges ++= changes
  }

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
