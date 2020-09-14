package de.hpi.dataset_versioning.db_synthesis.baseline

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, EntityFieldLineage, ProjectedTemporalRow, TemporalColumn, TemporalRow, TemporalTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, DecomposedTemporalTableIdentifier}
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.{DataBasedMatchCalculator, PairwiseTupleMapper, TemporalDatabaseTableTrait, TemporalSchemaMapper}
import de.hpi.dataset_versioning.db_synthesis.baseline.index.ValueLineageIndex
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.{BinaryReadable, BinarySerializable, SynthesizedTemporalDatabaseTableSketch, TemporalColumnTrait, TemporalFieldTrait}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
class SynthesizedTemporalDatabaseTable(val id:String,
                                       unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                       val schema: collection.IndexedSeq[AttributeLineage],
                                       val keyAttributeLineages: collection.Set[AttributeLineage],
                                       val rows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer(),
                                       val schemaTracking:mutable.HashMap[AttributeLineage,mutable.HashMap[DecomposedTemporalTableIdentifier,mutable.HashSet[Int]]],
                                       val uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID())
  extends AbstractTemporalDatabaseTable[Any](unionedTables) with StrictLogging with BinarySerializable{

  def tracksEntityMapping: Boolean = true

  def numChanges = rows.map(tr => tr.fields.map(_.changeCount).sum).sum


  def writeToStandardTemporaryFile() = {
    val f = DBSynthesis_IOService.getSynthesizedTableTempFile(uniqueSynthTableID)
    writeToBinaryFile(f)
  }

  def writeToStandardFinalDatabaseFile() = {
    val f = DBSynthesis_IOService.getSynthesizedTableInFinalDatabaseFile(uniqueSynthTableID)
    writeToBinaryFile(f)
  }

  override def columns: IndexedSeq[TemporalColumnTrait[Any]] = {
    val a = (0 until schema.size).map(attrIndex => {
      val col = rows.map(tr => new EntityFieldLineage(tr.entityID,tr.fields(attrIndex)))
      new TemporalColumn(id,schema(attrIndex),col)
    })
    a
  }

  def getActiveTime = {
    schema.map(_.activeTimeIntervals).reduce((a,b) => a.union(b))
  }

  def nonKeyAttributeLineages = schema.filter(al => !keyAttributeLineages.contains(al))

  var keyIsArtificial = false

  def getBestMergeMapping(curCandidate: DecomposedTemporalTable) = {
    val greedyMatcher = new SuperSimpleGreedySchemaMatcher(this,curCandidate)
    greedyMatcher.getBestSchemaMatching()
  }

  override def primaryKey = keyAttributeLineages

  override def getID: String = id

  override def nrows: Int = columns.head.fieldLineages.size

  override def buildTemporalColumn(unionedColID: String,
                                   unionedAttrLineage: AttributeLineage,
                                   unionedFieldLineages: ArrayBuffer[TemporalFieldTrait[Any]],
                                   unionedTableID:String): TemporalColumnTrait[Any] = {
    new TemporalColumn(unionedColID,
      unionedAttrLineage,
      unionedFieldLineages.toIndexedSeq.zipWithIndex.map(t => EntityFieldLineage(t._2,ValueLineage(t._1.getValueLineage))))
  }

  override def buildUnionedTable(unionedTableID: String,
                             unionedTables: mutable.HashSet[DecomposedTemporalTableIdentifier],
                             pkIDSet: collection.Set[Int],
                             newTcSketches: Array[TemporalColumnTrait[Any]],
                             other: TemporalDatabaseTableTrait[Any],
                             myTupleIndicesToNewTupleIndices:collection.Map[Int,Int],
                             otherTupleIndicesToNewTupleIndices:collection.Map[Int,Int],
                             newColumnIDToMyColumns:collection.Map[Int,Set[AttributeLineage]],
                             newColumnIDToOtherColumns:collection.Map[Int,Set[AttributeLineage]]): TemporalDatabaseTableTrait[Any] ={
    val newAttrsByID = newTcSketches.map(tcs => (tcs.attributeLineage.attrId,tcs.attributeLineage)).toMap
    val rowsInUnionedTable:IndexedSeq[TemporalRow] = (0 until newTcSketches.head.fieldLineages.size).map(rID => {
      val fields = newTcSketches.map(tc => tc.fieldLineages(rID).asInstanceOf[ValueLineage])
      val newRow = new SynthesizedTemporalRow(rID,fields,mutable.HashMap(),mutable.HashMap())
      newRow
    })
    addToTupleMapping(rows,myTupleIndicesToNewTupleIndices, rowsInUnionedTable)
    addToTupleMapping(other.asInstanceOf[SynthesizedTemporalDatabaseTable].rows,otherTupleIndicesToNewTupleIndices, rowsInUnionedTable)
    val newSchemaMapping = mutable.HashMap[AttributeLineage,mutable.HashMap[DecomposedTemporalTableIdentifier,mutable.HashSet[Int]]]()
    val newSchema = newTcSketches.map(_.attributeLineage)
    newSchema.foreach(al => {
      val oldColsLeft = newColumnIDToMyColumns(al.attrId)
      addAttributeTrackings(newSchemaMapping,schemaTracking,al, oldColsLeft)
      val oldColsRight = newColumnIDToOtherColumns(al.attrId)
      addAttributeTrackings(newSchemaMapping,other.asInstanceOf[SynthesizedTemporalDatabaseTable].schemaTracking,al, oldColsRight)
    })
    new SynthesizedTemporalDatabaseTable(unionedTableID,
      unionedTables,
      newSchema,
      pkIDSet.map(id => newAttrsByID(id)),
      mutable.ArrayBuffer() ++ rowsInUnionedTable,
      newSchemaMapping
    )
  }

  private def addAttributeTrackings(newAttributeTracking:mutable.HashMap[AttributeLineage,mutable.HashMap[DecomposedTemporalTableIdentifier, mutable.HashSet[Int]]],
                                    oldAttributeTracking:mutable.HashMap[AttributeLineage,mutable.HashMap[DecomposedTemporalTableIdentifier, mutable.HashSet[Int]]],
                                    newAttributeLineage: AttributeLineage,
                                    oldAttributeLineages: Set[AttributeLineage]) = {
    oldAttributeLineages.foreach(alOld => {
      val mapToAdjust = newAttributeTracking.getOrElseUpdate(newAttributeLineage, mutable.HashMap[DecomposedTemporalTableIdentifier, mutable.HashSet[Int]]())
      oldAttributeTracking(alOld).foreach { case (dttID, colSet) => {
        mapToAdjust.getOrElseUpdate(dttID, mutable.HashSet[Int]()).addAll(colSet)
      }
      }
    })
  }

  private def addToTupleMapping(rows:ArrayBuffer[TemporalRow], leftTupleIndicesToNewTupleIndices: collection.Map[Int, Int], rowsInUnionedTable: IndexedSeq[TemporalRow]) = {
    for (i <- 0 until rows.size) {
      val myRow = rows(i).asInstanceOf[SynthesizedTemporalRow]
      val mappedRow = rowsInUnionedTable(leftTupleIndicesToNewTupleIndices(i)).asInstanceOf[SynthesizedTemporalRow]
      myRow.tupleIDToDTTTupleID.foreach { case (id, tupleID) => {
        assert(!mappedRow.tupleIDToDTTTupleID.contains(id)) //if this does not hold our fundamental assumption about how the unioning works is wrong and the implementation needs to be adapted (think why this happens before changing it to a set!)
        mappedRow.tupleIDToDTTTupleID(id) = tupleID
      }
      }
      myRow.tupleIDTOViewTupleIDs.foreach { case (viewID, tupleIDs) => {
        mappedRow.tupleIDTOViewTupleIDs.getOrElseUpdate(viewID, mutable.HashSet[Long]()).addAll(tupleIDs)
      }
      }
    }
  }

  override def informativeTableName: String = getID + "(" + schema.map(_.lastName).mkString(",") + ")"

  override def fieldValueAtTimestamp(rowIndex: Int, colIndex: Int, ts:LocalDate): Any = rows(rowIndex).fields(colIndex).valueAt(ts)

  override def fieldIsWildcardAt(rowIndex: Int, colIndex: Int, ts: LocalDate): Boolean = {
    ValueLineage.isWildcard(fieldValueAtTimestamp(rowIndex, colIndex, ts))
  }

  override def getTuple(rowIndex: Int): IndexedSeq[TemporalFieldTrait[Any]] = rows(rowIndex).fields.map(_.asInstanceOf[TemporalFieldTrait[Any]]).toIndexedSeq
}
object SynthesizedTemporalDatabaseTable extends BinaryReadable[SynthesizedTemporalDatabaseTable] with StrictLogging {
  logger.debug("Potential Optimization: The columns method (in synth table), the columns have to be generated from the row representation - if this is too slow, it would be good to revisit this")

  def loadFromStandardFile(id:Int) = loadFromFile(DBSynthesis_IOService.getSynthesizedTableTempFile(id))

  def initFrom(dttToMerge: DecomposedTemporalTable) = {
    val synthesizedSchema = dttToMerge.containedAttrLineages
    var curEntityID:Long = 0
    val entityIDMatchingSynthesizedToOriginal = mutable.HashMap[Long,Long]()
    val tt = TemporalTable.loadAndCache(dttToMerge.id.viewID) //TODO: we will need to shrink this cache at some point
      .project(dttToMerge)
    val newRows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer()
    tt.projection.rows.foreach(tr => {
      val projectedTemporalRow = tr.asInstanceOf[ProjectedTemporalRow]
      val originalIds = mutable.HashMap[DecomposedTemporalTableIdentifier,Long](tt.projection.dttID.get.id -> tr.entityID)
      val toViewIds = mutable.HashMap[String,mutable.HashSet[Long]](tt.original.id -> (mutable.HashSet() ++ projectedTemporalRow.mappedEntityIds))
      newRows.addOne(new SynthesizedTemporalRow(curEntityID,tr.fields,originalIds,toViewIds))
      entityIDMatchingSynthesizedToOriginal.put(curEntityID,tr.entityID)
      curEntityID +=1
    })
    val attributeTrackingSynthesizedToDTT = mutable.HashMap() ++ synthesizedSchema.map(al => {
      val map = mutable.HashMap[DecomposedTemporalTableIdentifier,mutable.HashSet[Int]]()
      map.put(tt.projection.dttID.get.id,mutable.HashSet(al.attrId))
      (al,map)
    }).toMap
    val synthTable = new SynthesizedTemporalDatabaseTable(dttToMerge.compositeID,
      mutable.HashSet(dttToMerge.id),
      synthesizedSchema,
      dttToMerge.primaryKey,
      newRows,
      attributeTrackingSynthesizedToDTT
    )
    synthTable
  }

}