package de.hpi.dataset_versioning.data.change

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.bottom_up.{FieldLineage, ValueLineage}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}
import de.metanome.algorithm_integration.ColumnIdentifier

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

class TemporalTable(val id:String,val attributes:collection.IndexedSeq[AttributeLineage],val rows:collection.IndexedSeq[TemporalRow]){

  val timestampsWithChanges = {
    val timestampsSet = mutable.HashSet[LocalDate]()
    rows.foreach(_.fields.foreach(vl => timestampsSet.addAll(vl.lineage.keys)))
    timestampsSet.toIndexedSeq
      .sortBy(_.toEpochDay)
  }

  def allFields = rows.flatMap(_.fields)

  def allValuesNEAt(ts: LocalDate): Boolean = {
      allFields.forall(vl => vl.valueAt(ts)==ReservedChangeValues.NOT_EXISTANT)
  }

  val activeTimeIntervals = {
    var curBegin = timestampsWithChanges(0)
    var curEnd:LocalDate = null
    var activeTimeIntervals = mutable.ArrayBuffer[TimeInterval]()
    for(ts <- timestampsWithChanges.tail){
      if(curEnd == null && allValuesNEAt(ts)){
        assert(curBegin!=null)
        activeTimeIntervals.append(TimeInterval(curBegin,Some(ts)))
        curBegin=null
        curEnd=null
      } else if(curBegin==null){
        assert(!allValuesNEAt(ts))
        curBegin = ts
      }
    }
    //add last time period
    if(curBegin!=null)
      activeTimeIntervals += TimeInterval(curBegin,None)
    activeTimeIntervals
  }

  def buildIndexByChangeTimestamp = {
    val index:Map[LocalDate,mutable.HashSet[(Int,Int)]] = timestampsWithChanges
      .map(t => (t,mutable.HashSet[(Int,Int)]()))
      .toMap
    for (i <- 0 until rows.size){
      val fields = rows(i).fields
      for(j <- 0 until fields.size){
        fields(j).lineage.keys.foreach( t => index(t).add((i,j)))
      }
    }
    index
  }

  def getFieldLineageReferences = {
    (0 until rows.size).flatMap(i => (0 until attributes.size).map(j => FieldLineageReference(this,i,j)))
  }

}

object TemporalTable extends StrictLogging{
  def load(id: String) = from(ChangeCube.load(id))


  def from(cube: ChangeCube) = {
    //TODO: column order in temporal table - we can do better if we have to (?)
    val attributes = cube.colIDTOAttributeMap
      .toIndexedSeq
      .sortBy(t => {
        val latestTimestamp = t._2.keys.maxBy(_.toEpochDay)
        t._2(latestTimestamp).position.getOrElse(Int.MaxValue)
      })
      .map{case (colID,attributeMap) => {
        new AttributeLineage(colID,
          mutable.TreeMap[LocalDate,AttributeState]().addAll(attributeMap.map(t => (t._1,AttributeState(Some(t._2))))))
      }}
      val colIdToPosition = attributes
        .zipWithIndex
        .map{case (al,pos) => (al.attrId,pos)}
        .toMap
    val ncols = colIdToPosition.size
    val allChanges = cube.allChanges
    val allEntityIDs = allChanges.map(c => c.e)
      .toSet
      .toIndexedSeq
      .sorted
    if(allEntityIDs != (0 until allEntityIDs.size)){
      logger.warn("entity ids are not strictly increasing numbers")
    }
    val rows = mutable.ArrayBuffer[TemporalRow]()
    //create blank rows:
    val entityIDToRowIndex = allEntityIDs.zipWithIndex.toMap
    allEntityIDs.foreach(e => rows += new TemporalRow(e,mutable.IndexedSeq.fill[ValueLineage](ncols)(new ValueLineage())))
    //fill them with content
    val allTimestamps = mutable.TreeSet[LocalDate]()
    allChanges.foreach(c => {
      val colPosition = colIdToPosition(c.pID)
      rows(entityIDToRowIndex(c.e)).fields(colPosition).lineage.put(c.t,c.newValue)
      allTimestamps.add(c.t)
    })
    //TODO: adapt attribute lineage representation to this one
    val attributesCleaned = attributes.zipWithIndex.map{case (al,i) => {
      val newLineage = mutable.TreeMap[LocalDate,AttributeState]()
      newLineage += (al.lineage.head)
      var (prevTs,prevAttr) = al.lineage.head
      var prevWasNE = false
      allTimestamps
        .filter(ts => ts.isAfter(al.lineage.head._1))
        .foreach(curTs => {
          if(!al.lineage.contains(curTs) && !prevWasNE) {
            newLineage.put(curTs,AttributeState(None))
            prevWasNE = true
          } else if(al.lineage.contains(curTs) && prevWasNE){
            val curAttr = al.lineage(curTs)
            newLineage.put(curTs,curAttr)
            prevAttr = curAttr
            prevWasNE = false
          } else if(al.lineage.contains(curTs)){
            val curAttr = al.lineage(curTs)
            if(prevAttr != curAttr)
              newLineage.put(curTs,curAttr)
            prevAttr = curAttr
          }
      })
      new AttributeLineage(al.attrId,newLineage)
    }}
    new TemporalTable(cube.datasetID,attributesCleaned,rows)
  }
}
