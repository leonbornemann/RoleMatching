package de.hpi.dataset_versioning.data.change.temporal_tables

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, AttributeState, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.data.change.temporal_tables.time.TimeInterval
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.{EntityFieldLineage, FieldLineageReference, TemporalRow, ValueLineage}
import de.hpi.dataset_versioning.data.change.{ChangeCube, ReservedChangeValues}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based.TableChangeCounter
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFSurrogateReferenceRow, BCNFSurrogateReferenceTable, BCNFTableSchema}
import de.hpi.dataset_versioning.db_synthesis.sketches.{BinaryReadable, BinarySerializable}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import java.time.LocalDate
import scala.collection.mutable

@SerialVersionUID(3L)
class TemporalTable(val id:String,
                    val attributes:collection.IndexedSeq[AttributeLineage],
                    val rows:collection.IndexedSeq[TemporalRow],
                    val dtt:Option[SurrogateBasedDecomposedTemporalTable] = None) extends Serializable with BinarySerializable{

//  assert(attributes.size>0)
//  if(rows.size==0){
//    println(id)
//    println(dtt)
//  }

  def writeTOBCNFTemporalTableFile = {
    assert(dtt.isDefined && !dtt.get.isAssociation)
    writeToBinaryFile(DBSynthesis_IOService.getOptimizationBCNFTemporalTableFile(dtt.get.id))
  }

  def hasSurrogateValues = surrogateAttributes.size!=0 && surrogateRows.size!=0

  var surrogateAttributes = IndexedSeq[SurrogateAttributeLineage]()
  var surrogateRows = scala.collection.mutable.ArrayBuffer[IndexedSeq[Int]]()

  def tableSchemaString = id + "(" + attributes.map(_.lastName).mkString(",")+")"

  def insertTime = rows.flatMap(r => r.fields.flatMap(vl => vl.lineage).filter(t => !ValueLineage.isWildcard(t._2)).map(_._1)).minBy(_.toEpochDay)

  def countChanges(changeCounter: TableChangeCounter, pkSet:Set[Int]): (Int,Int) = {
    changeCounter.countChanges(this)
  }

  def addSurrogates(surrogateKeys: Set[SurrogateAttributeLineage]) = {
    //fill with content:
    assert(!hasSurrogateValues)
    val cols = getTemporalColumns().map(tc => (tc.attrID,tc)).toMap
    val surrogateKeysOrdered = surrogateKeys.toIndexedSeq.sortBy(_.surrogateID)
    surrogateAttributes = surrogateKeysOrdered
    val surrogateColsOrdered = surrogateKeysOrdered.map(sk => {
      val col = cols(sk.referencedAttrId)
      assert(col.fieldLineages.size==rows.size)
      val valToKey = mutable.HashMap[ValueLineage,Int]()
      var curKeyValue = 0
      val surrogateCol = col.fieldLineages.map(vl => {
        if(valToKey.contains(vl))
          valToKey(vl)
        else {
          valToKey.put(vl,curKeyValue)
          curKeyValue +=1
          curKeyValue-1
        }
      })
      surrogateCol
    })
    for( rowIndex <- 0 until rows.size){
      val surrogateKeyValuesForCurRow = (0 until surrogateColsOrdered.size).map( colIndex => surrogateColsOrdered(colIndex)(rowIndex))
      surrogateRows += surrogateKeyValuesForCurRow
    }
  }

  def serializeToStandardBinaryFile() = {
    val file = IOService.getTemporalTableBinaryFile(id,if(dtt.isDefined) Some(dtt.get.id) else None)
    writeToBinaryFile(file)
  }

  def isProjection = dtt.isDefined

  def writeTableSketch = {
    ???
//    assert(isProjection)
//    val tcs = getTemporalColumns()
//    val firstEntityIds = tcs.head.lineages.map(_.entityID)
//    assert(tcs.forall(tc => tc.lineages.map(_.entityID)==firstEntityIds))
//    val dttSketch = new DecomposedTemporalTableSketch(dtt.get.id,primaryKey,mutable.HashSet(),tcs.map(tc => TemporalColumnSketch.from(tc)).toArray)
//    dttSketch.writeToStandardFile()
  }

  def project(bcnfTable:BCNFTableSchema, associations: Iterable[AssociationSchema]) = {
    val associationResults = associations.map(association => {
      assert(surrogateAttributes.contains(association.surrogateKey))
      val newRows:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRow] = collection.mutable.ArrayBuffer()
      val newPKRows:collection.mutable.ArrayBuffer[IndexedSeq[Int]] = collection.mutable.ArrayBuffer()
      //primary key surrogate in new table:
      //attributes in new table:
      val oldAttributePosition = this.attributes.map(_.attrId).indexOf(association.attributeLineage.attrId)
      //foreign key surrogate in new table:
      var newSurrogateKeyCounter = 0
      val valueSet = mutable.HashMap[ValueLineage,Int]()
      val surrogateKeyColumnValues = mutable.ArrayBuffer[Int]()
      this.rows.foreach{case (tr) => {
        //row content:
        val newRowContent = tr.fields(oldAttributePosition)
        //primary key content:
        var newPKContent = -1
        if(valueSet.contains(newRowContent)){
          newPKContent = valueSet(newRowContent)
        } else {
          newSurrogateKeyCounter +=1
          newPKContent = newSurrogateKeyCounter-1
          valueSet.put(newRowContent,newPKContent)
          val newRow = new SurrogateBasedTemporalRow(IndexedSeq(newPKContent),newRowContent,IndexedSeq())
          newRows.addOne(newRow)
          newPKRows.addOne(IndexedSeq(newPKContent))
        }
        surrogateKeyColumnValues +=newPKContent
      }}
      val projectedAssociationTable = new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(association.id.compositeID,
        mutable.HashSet(),
        mutable.HashSet(association.id),
        IndexedSeq(association.surrogateKey),
        association.attributeLineage,
        IndexedSeq(),
        newRows
      )
      (projectedAssociationTable,surrogateKeyColumnValues)
    }).toIndexedSeq
    val surrogateAttrIDToPos = surrogateAttributes.zipWithIndex.map{case (s,i) => (s.surrogateID,i)}.toMap
    val surrogateKeyIDs = bcnfTable.surrogateKey.map(s => s.surrogateID)
    val surrogateFkIds = bcnfTable.foreignSurrogateKeysToReferencedBCNFTables.map(_._1.surrogateID)
    val bcnfReferenceTableRows = (0 until rows.size).map(rowIndex => {
      val associationReferenceValues = mutable.IndexedSeq() ++ associationResults.map{case (st,surogateValues) => {
        surogateValues(rowIndex)
      }}
      val pkRow = mutable.IndexedSeq() ++ surrogateKeyIDs.map(id => surrogateRows(rowIndex)(surrogateAttrIDToPos(id)))
      val fkRow = surrogateFkIds.map(id => surrogateRows(rowIndex)(surrogateAttrIDToPos(id)))
      new BCNFSurrogateReferenceRow(pkRow,associationReferenceValues,fkRow)
    })
    val bcnfReferenceTable = new BCNFSurrogateReferenceTable(bcnfTable,
      associationResults.map(_._1.key.head),
      bcnfReferenceTableRows
    )
    (bcnfReferenceTable,associationResults.map(_._1))
  }


  def project(dttToMerge: SurrogateBasedDecomposedTemporalTable) = {
    assert(dttToMerge.allSurrogates.forall(s => surrogateAttributes.contains(s)))
    val newSchema = dttToMerge.attributes
    val newRows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer()
    val newPKRows:collection.mutable.ArrayBuffer[IndexedSeq[Int]] = collection.mutable.ArrayBuffer()
    val newFKRows:collection.mutable.ArrayBuffer[IndexedSeq[Int]] = collection.mutable.ArrayBuffer()
    //primary key surrogate in new table:
    val surrogateKeyToPosInNewTable = dttToMerge.surrogateKey.zipWithIndex.map{case (al,i) => (al.referencedAttrId,i)}.toMap
    val oldSurogateKeyPositionToNewSurrogatePosition = this.surrogateAttributes.zipWithIndex
      .withFilter(s => surrogateKeyToPosInNewTable.contains(s._1.referencedAttrId))
      .map{case (al,i) => (i,surrogateKeyToPosInNewTable(al.referencedAttrId))}.toIndexedSeq
    //attributes in new table:
    val alIDToPosInNewTable = dttToMerge.attributes.zipWithIndex.map{case (al,i) => (al.attrId,i)}.toMap
    val oldAttributePositionToNewAttributePosition = this.attributes.zipWithIndex
      .withFilter(al => alIDToPosInNewTable.contains(al._1.attrId))
      .map{case (al,i) => (i,alIDToPosInNewTable(al.attrId))}.toIndexedSeq
    //foreign key surrogate in new table:
    val surrogateForeignKEyToPosInNewTable = dttToMerge.foreignSurrogateKeysToReferencedTables.zipWithIndex.map{case (al,i) => (al._1.referencedAttrId,i)}.toMap
    val oldForeignKeySurogatePositionToNewForeignKeySurrogatePosition = this.surrogateAttributes.zipWithIndex
      .withFilter(s => surrogateForeignKEyToPosInNewTable.contains(s._1.referencedAttrId))
      .map{case (al,i) => (i,surrogateForeignKEyToPosInNewTable(al.referencedAttrId))}.toIndexedSeq
    val byPK = new mutable.HashMap[collection.IndexedSeq[Int],(TemporalRow,collection.IndexedSeq[Int])]()
    this.rows.zipWithIndex.foreach{case (tr,rowIndex) => {
      //row content:
      val newRowContent = oldAttributePositionToNewAttributePosition.map{case (oldIndex,newIndex) => {
        (newIndex,tr.fields(oldIndex))
      }}.sortBy(_._1)
      assert(newRowContent.map(_._1) == (0 until newSchema.size))
      val newRow = new TemporalRow(tr.entityID,newRowContent.map(_._2))
      //primary key content:
      val newPKContent = oldSurogateKeyPositionToNewSurrogatePosition.map{case (oldIndex,newIndex) => {
        (newIndex,surrogateRows(rowIndex)(oldIndex))
      }}.sortBy(_._1)
      val newPKRow = newPKContent.map(_._2)
      //foreign key content:
      val newFKContent = oldForeignKeySurogatePositionToNewForeignKeySurrogatePosition.map{case (oldIndex,newIndex) => {
        (newIndex,surrogateRows(rowIndex)(oldIndex))
      }}.sortBy(_._1)
      val newFKRow = newFKContent.map(_._2)
      if(byPK.contains(newPKRow)){
        val (mappedRow,mappedFKRow) = byPK(newPKRow)
        if(!(newRow.fields == mappedRow.fields && mappedFKRow == newFKRow)){
          println(id)
          println(dttToMerge.id)
          println(dttToMerge.surrogateKey)
          println(dttToMerge.attributes.size)
          println(dttToMerge.foreignSurrogateKeysToReferencedTables.size)
        }
        assert(newRow.fields == mappedRow.fields && mappedFKRow == newFKRow)
      } else {
        byPK.put(newPKRow,(newRow,newFKRow))
        newRows.addOne(newRow)
        newPKRows.addOne(newPKRow)
        newFKRows.addOne(newFKRow)
      }
    }}
    val projectedTable = new TemporalTable(dttToMerge.compositeID,newSchema,newRows,Some(dttToMerge))
    val newSurrogateKeyAttributes = oldSurogateKeyPositionToNewSurrogatePosition.sortBy(_._2).map{case (oldI,_) => surrogateAttributes(oldI)}
    val newSurrogateFKAttributes = oldForeignKeySurogatePositionToNewForeignKeySurrogatePosition.sortBy(_._2).map{case (oldI,_) => surrogateAttributes(oldI)}
    projectedTable.surrogateAttributes = newSurrogateKeyAttributes ++ newSurrogateFKAttributes
    projectedTable.surrogateRows = newPKRows.zip(newFKRows).map{case (pk,fk) =>pk++fk }
    new ProjectedTemporalTable(projectedTable,this)
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

  def activeTimeIntervals = {
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

  def loadBCNFFromStandardBinaryFile(id:DecomposedTemporalTableIdentifier) = {
    assert(!id.associationID.isDefined)
    loadFromFile(DBSynthesis_IOService.getOptimizationBCNFTemporalTableFile(id))
  }

  def bcnfContentTableExists(id:DecomposedTemporalTableIdentifier) = {
    DBSynthesis_IOService.getOptimizationBCNFTemporalTableFile(id).exists()
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
