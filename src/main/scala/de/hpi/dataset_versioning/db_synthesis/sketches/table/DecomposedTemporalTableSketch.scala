package de.hpi.dataset_versioning.db_synthesis.sketches.table

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.sketches.column.{TemporalColumnSketch, TemporalColumnTrait}
import de.hpi.dataset_versioning.db_synthesis.sketches.field.{TemporalFieldTrait, Variant2Sketch}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
class DecomposedTemporalTableSketch(val tableID:DecomposedTemporalTableIdentifier,
                                   primaryKeyIDs:Set[Int],
                                    unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                    temporalColumnSketches:Array[TemporalColumnSketch]) extends TemporalTableSketch(unionedTables,temporalColumnSketches) with Serializable{

  def writeToStandardFile() = {
    val temporalTableFile = DBSynthesis_IOService.getDecomposedTemporalTableSketchFile(tableID,temporalColumnSketches.head.fieldLineageSketches.head.getVariantName)
    writeToBinaryFile(temporalTableFile)
  }

  override def getID: String = tableID.compositeID

  override def dataColumns: IndexedSeq[TemporalColumnTrait[Int]] = temporalColumnSketches.toIndexedSeq

  override def primaryKey: collection.Set[AttributeLineage] = temporalColumnSketches
    .map(_.attributeLineage)
    .filter(al => primaryKeyIDs.contains(al.attrId)).toSet

  override def dataAttributeLineages: collection.IndexedSeq[AttributeLineage] = temporalColumnSketches
    .map(_.attributeLineage)
    .filter(al => !primaryKeyIDs.contains(al.attrId))

  override def nrows: Int = dataColumns.head.fieldLineages.size

  override def buildTemporalColumn(unionedColID: String, unionedAttrLineage: AttributeLineage, unionedFieldLineages: ArrayBuffer[TemporalFieldTrait[Int]],unionedTableID:String): TemporalColumnTrait[Int] = {
    throw new AssertionError("This should never be called on this class")
  }

  def buildUnionedTable(unionedTableID: String,
                        unionedTables: mutable.HashSet[DecomposedTemporalTableIdentifier],
                        pkIDSet: collection.Set[Int],
                        columns: Array[TemporalColumnTrait[Int]],
                        other: TemporalDatabaseTableTrait[Int],
                        leftTupleIndicesToNewTupleIndices:collection.Map[Int,Int],
                        rightTupleIndicesToNewTupleIndices:collection.Map[Int,Int],
                        newColumnIDToOldColumnsLeft:collection.Map[Int,Set[AttributeLineage]],
                        newColumnIDToOldColumnsRight:collection.Map[Int,Set[AttributeLineage]]): TemporalDatabaseTableTrait[Int] = {
    throw new AssertionError("This should never be called on this class")
  }

  override def informativeTableName: String = getID + "(" + temporalColumnSketches.map(_.attributeLineage.lastName).mkString(",") + ")"

  override def tracksEntityMapping: Boolean = false

  override def fieldIsWildcardAt(rowIndex: Int, colIndex: Int, ts: LocalDate): Boolean = {
    val lineage = temporalColumnSketches(colIndex).fieldLineageSketches(colIndex)
    ValueLineage.isWildcard(lineage.valueAt(ts))
  }

  override def fieldValueAtTimestamp(rowIndex: Int, colIndex: Int, ts: LocalDate): Int = temporalColumnSketches(colIndex).fieldLineageSketches(colIndex).valueAt(ts)

  override def getDataTuple(rowIndex: Int): collection.IndexedSeq[TemporalFieldTrait[Int]] = dataColumns.map(c => c.fieldLineages(rowIndex))
}

object DecomposedTemporalTableSketch{

  def load(tableID:DecomposedTemporalTableIdentifier,variantString:String):DecomposedTemporalTableSketch = {
    val sketchFile = DBSynthesis_IOService.getDecomposedTemporalTableSketchFile(tableID,variantString)
    TemporalTableSketch.loadFromFile[DecomposedTemporalTableSketch](sketchFile)
  }

  def load(tableID:DecomposedTemporalTableIdentifier):DecomposedTemporalTableSketch = {
    load(tableID,Variant2Sketch.getVariantName)
  }

}
