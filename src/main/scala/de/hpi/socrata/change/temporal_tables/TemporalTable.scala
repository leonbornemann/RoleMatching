package de.hpi.socrata.change.temporal_tables

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.change.temporal_tables.attribute.{AttributeLineage, AttributeState, SurrogateAttributeLineage}
import de.hpi.socrata.change.temporal_tables.tuple.TemporalRow
import de.hpi.socrata.change.{ChangeCube, UpdateChangeCounter}
import de.hpi.socrata.io.Socrata_Synthesis_IOService
import de.hpi.socrata.tfmp_input.association.AssociationIdentifier
import de.hpi.socrata.tfmp_input.table.nonSketch.FactLineage
import de.hpi.socrata.tfmp_input.{BinaryReadable, BinarySerializable}

import java.time.LocalDate
import scala.collection.mutable

@SerialVersionUID(3L)
class TemporalTable(val id:String,
                    val attributes:collection.IndexedSeq[AttributeLineage],
                    val rows:collection.IndexedSeq[TemporalRow]) extends Serializable with BinarySerializable{

  def hasSurrogateValues = surrogateAttributes.size!=0 && surrogateRows.size!=0

  var surrogateAttributes = IndexedSeq[SurrogateAttributeLineage]()
  var surrogateRows = scala.collection.mutable.ArrayBuffer[IndexedSeq[Int]]()

  def tableSchemaString = id + "(" + attributes.map(_.lastName).mkString(",")+")"

  def insertTime = rows.flatMap(r => r.fields.flatMap(vl => vl.lineage).filter(t => !FactLineage.isWildcard(t._2)).map(_._1)).minBy(_.toEpochDay)

  def countChanges(changeCounter: UpdateChangeCounter, pkSet:Set[Int]): (Int,Int) = {
    changeCounter.countChanges(this)
  }

  val timestampsWithChanges = {
    val timestampsSet = mutable.HashSet[LocalDate]()
    rows.foreach(_.fields.foreach(vl => timestampsSet.addAll(vl.lineage.keys)))
    timestampsSet.toIndexedSeq
      .sortBy(_.toEpochDay)
  }

  def allFields = rows.flatMap(_.fields)

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

}

object TemporalTable extends StrictLogging with BinaryReadable[TemporalTable]{

  val cache = mutable.HashMap[String,TemporalTable]()

  def loadAndCache(originalID: String) = {
    cache.getOrElseUpdate(originalID,load(originalID))
  }

  def loadBCNFFromStandardBinaryFile(id:AssociationIdentifier) = {
    assert(!id.associationID.isDefined)
    loadFromFile(Socrata_Synthesis_IOService.getOptimizationBCNFTemporalTableFile(id))
  }

  def bcnfContentTableExists(id:AssociationIdentifier) = {
    Socrata_Synthesis_IOService.getOptimizationBCNFTemporalTableFile(id).exists()
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
    allEntityIDs.foreach(e => rows += new TemporalRow(e,mutable.IndexedSeq.fill[FactLineage](ncols)(new FactLineage())))
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
