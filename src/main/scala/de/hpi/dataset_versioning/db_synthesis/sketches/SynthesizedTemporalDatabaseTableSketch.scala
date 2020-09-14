package de.hpi.dataset_versioning.db_synthesis.sketches

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.TableUnionMatch
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, DecomposedTemporalTableIdentifier}
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.util.TableFormatter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
class SynthesizedTemporalDatabaseTableSketch(id:String,
                                             unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                             pkIDs: collection.Set[Int],
                                             temporalColumnSketches:Array[TemporalColumnSketch]) extends TemporalTableSketch(unionedTables,temporalColumnSketches) with TemporalDatabaseTableTrait[Int]{

  def nrows: Int = temporalColumnSketches(0).fieldLineageSketches.size

  def primaryKey = schema.filter(al => pkIDs.contains(al.attrId)).toSet

  def schema = temporalColumnSketches.map(tc => tc.attributeLineage).toIndexedSeq

  def nonKeyAttributeLineages = schema.filter(al => !primaryKey.contains(al))

  override def columns: IndexedSeq[TemporalColumnTrait[Int]] = temporalColumnSketches

  override def getID: String = id

  override def buildTemporalColumn(unionedColID: String, unionedAttrLineage: AttributeLineage, unionedFieldLineages: ArrayBuffer[TemporalFieldTrait[Int]], unionedTableID: String): TemporalColumnTrait[Int] = {
    new TemporalColumnSketch(unionedTableID,
      unionedAttrLineage,
      unionedFieldLineages.map(tf => Variant2Sketch.fromTimestampToHash(tf.getValueLineage)).toArray)
  }

  override def buildUnionedTable(unionedTableID: String,
                                 unionedTables: mutable.HashSet[DecomposedTemporalTableIdentifier],
                                 pkIDSet: collection.Set[Int],
                                 columns: Array[TemporalColumnTrait[Int]],
                                 other: TemporalDatabaseTableTrait[Int],
                                 leftTupleIndicesToNewTupleIndices:collection.Map[Int,Int],
                                 rightTupleIndicesToNewTupleIndices:collection.Map[Int,Int],
                                 newColumnIDToOldColumnsLeft:collection.Map[Int,Set[AttributeLineage]],
                                 newColumnIDToOldColumnsRight:collection.Map[Int,Set[AttributeLineage]]) = {
    new SynthesizedTemporalDatabaseTableSketch(unionedTableID,unionedTables,pkIDSet,columns.map(tct =>
      tct.asInstanceOf[TemporalColumnSketch]))
      ///new TemporalColumnSketch(unionedTableID,tct.attributeLineage,tct.fieldLineages.map(tft => tft.asInstanceOf[FieldLineageSketch]).toArray)))
  }

  override def informativeTableName: String = getID + "(" + schema.map(_.lastName).mkString(",") + ")"

  override def tracksEntityMapping: Boolean = false

  override def fieldValueAtTimestamp(rowIndex: Int, colIndex: Int, ts: LocalDate): Int = temporalColumnSketches(colIndex).fieldLineageSketches(rowIndex).valueAt(ts)

  override def fieldIsWildcardAt(rowIndex: Int, colIndex: Int, ts: LocalDate): Boolean = fieldValueAtTimestamp(rowIndex, colIndex, ts) == Variant2Sketch.WILDCARD

  override def getTuple(rowIndex: Int): collection.IndexedSeq[TemporalFieldTrait[Int]] = columns.map(c => c.fieldLineages(rowIndex))
}
object SynthesizedTemporalDatabaseTableSketch{

  def initFrom(dttToMerge: DecomposedTemporalTable):SynthesizedTemporalDatabaseTableSketch = {
    val sketch = DecomposedTemporalTableSketch.load(dttToMerge.id)
    new SynthesizedTemporalDatabaseTableSketch(dttToMerge.compositeID,
      mutable.HashSet(dttToMerge.id),
      dttToMerge.primaryKey.map(_.attrId),
      sketch.temporalColumnSketches)
  }

}