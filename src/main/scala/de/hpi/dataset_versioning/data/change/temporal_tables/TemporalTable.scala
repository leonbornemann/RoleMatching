package de.hpi.dataset_versioning.data.change.temporal_tables

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.{ChangeCube, ReservedChangeValues}
import de.hpi.dataset_versioning.db_synthesis.baseline.config.{InitialInsertIgnoreFieldChangeCounter, TableChangeCounter}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, DecomposedTemporalTableIdentifier}
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.{BinaryReadable, BinarySerializable}
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnSketch
import de.hpi.dataset_versioning.db_synthesis.sketches.table.DecomposedTemporalTableSketch
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

@SerialVersionUID(3L)
class TemporalTable(val id:String,
                    val attributes:collection.IndexedSeq[AttributeLineage],
                    val rows:collection.IndexedSeq[TemporalRow],
                    val dtt:Option[DecomposedTemporalTable] = None) extends Serializable with BinarySerializable{
  def tableSchemaString = id + "(" + attributes.map(_.lastName).mkString(",")+")"

  def insertTime = rows.flatMap(r => r.fields.flatMap(vl => vl.lineage).filter(t => !ValueLineage.isWildcard(t._2)).map(_._1)).minBy(_.toEpochDay)

  def countChanges(changeCounter: TableChangeCounter, pkSet:Set[Int]): Long = {
    changeCounter.countChanges(this,pkSet)
  }

  def serializeToStandardBinaryFile() = {
    val file = IOService.getTemporalTableBinaryFile(id,if(dtt.isDefined) Some(dtt.get.id) else None)
    writeToBinaryFile(file)
  }

  def isProjection = dtt.isDefined

  def writeTableSketch(primaryKey:Set[Int]) = {
    assert(isProjection)
    val tcs = getTemporalColumns()
    val firstEntityIds = tcs.head.lineages.map(_.entityID)
    assert(tcs.forall(tc => tc.lineages.map(_.entityID)==firstEntityIds))
    val dttSketch = new DecomposedTemporalTableSketch(dtt.get.id,primaryKey,mutable.HashSet(),tcs.map(tc => TemporalColumnSketch.from(tc)).toArray)
    dttSketch.writeToStandardFile()
  }


  def project(dttToMerge: DecomposedTemporalTable) = {
    val newSchema = dttToMerge.containedAttrLineages
    val rows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer()
    val alIDToPosInNewTable = dttToMerge.containedAttrLineages.zipWithIndex.map{case (al,i) => (al.attrId,i)}.toMap
    val oldAttributePositionToNewAttributePosition = this.attributes.zipWithIndex
      .withFilter(al => alIDToPosInNewTable.contains(al._1.attrId))
      .map{case (al,i) => (i,alIDToPosInNewTable(al.attrId))}.toIndexedSeq
    this.rows.foreach(tr => {
      val newRowContent = oldAttributePositionToNewAttributePosition.map{case (oldIndex,newIndex) => {
        (newIndex,tr.fields(oldIndex))
      }}.sortBy(_._1)
      assert(newRowContent.map(_._1) == (0 until newSchema.size))
      val newRow = new TemporalRow(tr.entityID,newRowContent.map(_._2))
      rows.addOne(newRow)
    })
    //filter out duplicate rows:
    val bySameTupleLineages = rows.groupBy(tr => {
      val a = tr.fields.map(_.lineage)
      a
    })
    val rowsProjected:collection.mutable.ArrayBuffer[ProjectedTemporalRow] = collection.mutable.ArrayBuffer()
    bySameTupleLineages.foreach{case (k,v) => {
      val id = v.sortBy(_.entityID).head.entityID
      rowsProjected += new ProjectedTemporalRow(id,v.head.fields,v.map(_.entityID).toSet)
    }}
    new ProjectedTemporalTable(new TemporalTable(dttToMerge.compositeID,newSchema,rowsProjected,Some(dttToMerge)),this)
  }

  def getTemporalColumns() = {
    attributes.zipWithIndex.map{case (al,i) => {
      val lineages = rows.map(tr => EntityFieldLineage(tr.entityID,tr.fields(i)))
      new TemporalColumn(id,al,lineages)
    }}
  }

  val timestampsWithChanges = {
    val timestampsSet = mutable.HashSet[LocalDate]()
    rows.foreach(_.fields.foreach(vl => timestampsSet.addAll(vl.lineage.keys)))
    timestampsSet.toIndexedSeq
      .sortBy(_.toEpochDay)
  }

  def allFields = rows.flatMap(_.fields)

  def allValuesNEAt(ts: LocalDate): Boolean = {
      allFields.forall(vl => vl.valueAt(ts)==ReservedChangeValues.NOT_EXISTANT_ROW)
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

object TemporalTable extends StrictLogging with BinaryReadable[TemporalTable]{

  val cache = mutable.HashMap[String,TemporalTable]()

  def loadAndCache(originalID: String) = {
    cache.getOrElseUpdate(originalID,load(originalID))
  }

  def load(id: String) = {
    //this turned out to be not worth it - json is actually faster
//    if(!IOService.getTemporalTableBinaryFile(id,None).exists()) {
//      val result = from(ChangeCube.load(id))
//      result.serializeToStandardBinaryFile()
//      result
//    }
//    else {
//      loadFromFile(IOService.getTemporalTableBinaryFile(id,None))
//    }
    loadFromChangeCube(id)
  }

  def loadFromChangeCube(id:String) = {
    from(ChangeCube.load(id))
  }

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
      rows(entityIDToRowIndex(c.e)).fields(colPosition).lineage.put(c.t,c.value)
      allTimestamps.add(c.t)
    })
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
