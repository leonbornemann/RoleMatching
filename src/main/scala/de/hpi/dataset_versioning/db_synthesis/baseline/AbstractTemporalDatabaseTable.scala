package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.{TemporalDatabaseTableTrait, TupleMatching}
import de.hpi.dataset_versioning.db_synthesis.sketches.{FieldLineageSketch, SynthesizedTemporalDatabaseTableSketch, TemporalColumnSketch, TemporalColumnTrait, TemporalFieldTrait}
import de.hpi.dataset_versioning.oneshot.ReadNonZeroExitValIDsMain.a
import de.hpi.dataset_versioning.util.TableFormatter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
abstract class AbstractTemporalDatabaseTable[A](val unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier]) extends TemporalDatabaseTableTrait[A] with Serializable{

  override def primaryKeyIsValid: Boolean = {
    val pkCOlumns = columns.filter(c => primaryKey.contains(c.attributeLineage)).toIndexedSeq.sortBy(_.attributeLineage.attrId)
    val rows = (0 until nrows).map(rID => {
      val keyPartOfTemporalRow = pkCOlumns.map(c => c.fieldLineages(rID).getValueLineage)
      keyPartOfTemporalRow
    })
    rows.size == rows.toSet.size
  }

  def printTable = {
    val dataRows:Seq[Seq[Any]] = (0 until nrows).map(rID => columns.map(c => "[" + c.fieldLineages(rID).getValueLineage.map(_._2).mkString(",") + "]"))
    val schema = columns.map(_.attributeLineage)
    val keys = primaryKey
    val header:Seq[Seq[Any]] = Seq(schema.map(al => {
      val keyString = if (keys.contains(al)) " (Key)" else ""
      al.lastName + keyString
    } ))
    val rows:Seq[Seq[Any]] = header ++ dataRows
    println(TableFormatter.format(rows))
  }

  override def getUnionedTables = unionedTables

  def buildAttrUnion(colsLeft: Set[AttributeLineage],newID:Int) = {
    colsLeft.reduce( (a,b)=> a.unionDisjoint(b,newID))
  }

  def buildTemporalColumn(unionedColID: String, unionedAttrLineage: AttributeLineage, unionedFieldLineages: ArrayBuffer[TemporalFieldTrait[A]],unionedTableID:String) : TemporalColumnTrait[A]

  def tracksEntityMapping: Boolean

  def buildUnionedTemporalColumns(left: TemporalDatabaseTableTrait[A],
                                  right: TemporalDatabaseTableTrait[A],
                                  bestMatch: TableUnionMatch[A],
                                  unionedTableID:String) = {
    val mapping = bestMatch.schemaMapping.get
    val leftColsByID = left.columns.map(c => (c.attrID,c)).toMap
    val rightColsByID = right.columns.map(c => (c.attrID,c)).toMap
    assert(bestMatch.tupleMapping.isDefined)
    val tupleMapping = bestMatch.tupleMapping.get
    val leftTupleIndicesToNewTupleIndices = mutable.HashMap[Int,Int]()
    val rightTupleIndicesToNewTupleIndices = mutable.HashMap[Int,Int]()
    val newColumnIDToOldColumnsLeft = mutable.HashMap[Int,Set[AttributeLineage]]()
    val newColumnIDToOldColumnsRight = mutable.HashMap[Int,Set[AttributeLineage]]()
    val newColumns = mapping.map{case (colsLeft,colsRight) => {
      val newAttributeLineageID = colsLeft.map(_.attrId).min
      newColumnIDToOldColumnsLeft(newAttributeLineageID) = colsLeft
      newColumnIDToOldColumnsRight(newAttributeLineageID) = colsRight
      val attrLeft = buildAttrUnion(colsLeft,newAttributeLineageID) // that will be the final attribute
      val unionedFieldLineages = mutable.ArrayBuffer[TemporalFieldTrait[A]]()
      //lineages from left only:
      val unmatchedLeftSorted = tupleMapping.unmatchedTupleIndicesA.toIndexedSeq.sorted
      val unmatchedFieldsLeft = mergeFieldLineages(unmatchedLeftSorted,colsLeft,leftColsByID)
      unmatchedLeftSorted.zipWithIndex.foreach{case (rowIndexLeft,index) => {
        val newRowIndex = unionedFieldLineages.size + index
        addToIndexTracking(leftTupleIndicesToNewTupleIndices, newRowIndex, rowIndexLeft)
      }}
      unionedFieldLineages.addAll(unmatchedFieldsLeft)
      //lineages from right only:
      val unmatchedRightSorted = tupleMapping.unmatchedTupleIndicesB.toIndexedSeq.sorted
      unmatchedRightSorted.zipWithIndex.foreach{case(rowIndexRight,index) => {
        val newRowIndex = unionedFieldLineages.size + index
        addToIndexTracking(rightTupleIndicesToNewTupleIndices, newRowIndex, rowIndexRight)
      }}
      val unmatchedFieldsRight = mergeFieldLineages(unmatchedRightSorted,colsRight,rightColsByID)
      unionedFieldLineages.addAll(unmatchedFieldsRight)
      val mergedTuples = tupleMapping.matchedTuples.groupBy(_.tupleIndexA)
        .toIndexedSeq
        .sortBy(_._1)
        .zipWithIndex
        .map{case ((rowIndexLeft,rowIndicesRight),index) => {
          //tuple tracking for left table:
          val newRowIndex = unionedFieldLineages.size + index
          addToIndexTracking(leftTupleIndicesToNewTupleIndices, newRowIndex, rowIndexLeft)
          //tuple tracking for right table:
          rowIndicesRight.foreach{case TupleMatching(_, rowIndexRight, _) => {
            addToIndexTracking(rightTupleIndicesToNewTupleIndices, newRowIndex, rowIndexRight)
          }}
          val left = colsLeft.map(c => {
            leftColsByID(c.attrId).fieldLineages(rowIndexLeft)
          }).reduce((a, b) => a.mergeWithConsistent(b))
          val allRight = mergeFieldLineages(rowIndicesRight.map(_.tupleIndexB),colsRight,rightColsByID)
          val rightMerged = if(allRight.size>1) allRight.reduce((a,b) => a.mergeWithConsistent(b)) else allRight.head //is this really necessary?
          left.mergeWithConsistent(rightMerged)
        }}
      unionedFieldLineages.addAll(mergedTuples)
      val unionedColID = (colsLeft.toIndexedSeq.sortBy(_.attrId).map(al => al.attrId).mkString("_")
        + "_U_"
        + colsRight.toIndexedSeq.sortBy(_.attrId).map(al => al.attrId).mkString("_"))
      val unionedAttrLineage = attrLeft
      buildTemporalColumn(unionedColID, unionedAttrLineage,unionedFieldLineages,unionedTableID)
    }}
    (newColumns.toArray,
      leftTupleIndicesToNewTupleIndices,
      rightTupleIndicesToNewTupleIndices,
      newColumnIDToOldColumnsLeft,
      newColumnIDToOldColumnsRight)
  }

  private def addToIndexTracking(tupleIndicesToNewTupleIndices: mutable.HashMap[Int, Int], newRowIndex: Int, oldRowIndex: Int) = {
    if (tupleIndicesToNewTupleIndices.contains(oldRowIndex)) {
      if(tupleIndicesToNewTupleIndices(oldRowIndex)!=newRowIndex)
        println()
      assert(tupleIndicesToNewTupleIndices(oldRowIndex) == newRowIndex)
    } else {
      tupleIndicesToNewTupleIndices(oldRowIndex) = newRowIndex
    }
  }

  private def mergeFieldLineages(rowIndices: collection.IndexedSeq[Int],
                                 cols: Set[AttributeLineage],
                                 colsByID:Map[Int, TemporalColumnTrait[A]]) = {
    rowIndices.map(rowIndex => {
      //get values of all columns in cols Left
      val flLeft = cols.map(c => {
        colsByID(c.attrId).fieldLineages(rowIndex)
      }).reduce((a, b) => a.mergeWithConsistent(b))
      flLeft
    })
  }

  def buildUnionedTable(unionedTableID: String,
                        unionedTables: mutable.HashSet[DecomposedTemporalTableIdentifier],
                        pkIDSet: collection.Set[Int],
                        columns: Array[TemporalColumnTrait[A]],
                        other: TemporalDatabaseTableTrait[A],
                        leftTupleIndicesToNewTupleIndices:collection.Map[Int,Int],
                        rightTupleIndicesToNewTupleIndices:collection.Map[Int,Int],
                        newColumnIDToOldColumnsLeft:collection.Map[Int,Set[AttributeLineage]],
                        newColumnIDToOldColumnsRight:collection.Map[Int,Set[AttributeLineage]]): TemporalDatabaseTableTrait[A]

  def executeUnion(other: TemporalDatabaseTableTrait[A], bestMatch: TableUnionMatch[A]) :TemporalDatabaseTableTrait[A] = {
    var left:TemporalDatabaseTableTrait[A] = this
    var right = other
    if(bestMatch.firstMatchPartner==right){
      left = other
      right = this
    }
    assert(left == bestMatch.firstMatchPartner && right == bestMatch.secondMatchPartner)
    val unionedTableID = left.getID + "_UNION_" + right.getID
    val (newTcSketches,leftTupleIndicesToNewTupleIndices,rightTupleIndicesToNewTupleIndices,newColumnIDToOldColumnsLeft,newColumnIDToOldColumnsRight) = buildUnionedTemporalColumns(left,right,bestMatch,unionedTableID)
    val unionedTables = mutable.HashSet() ++ left.getUnionedTables ++ right.getUnionedTables
    val pkIDSet = left.primaryKey.map(_.attrId)
    val union:TemporalDatabaseTableTrait[A] = buildUnionedTable(unionedTableID,
      unionedTables,
      pkIDSet,
      newTcSketches,
      other,
      leftTupleIndicesToNewTupleIndices,
      rightTupleIndicesToNewTupleIndices,
      newColumnIDToOldColumnsLeft,
      newColumnIDToOldColumnsRight)
    union
  }

}
