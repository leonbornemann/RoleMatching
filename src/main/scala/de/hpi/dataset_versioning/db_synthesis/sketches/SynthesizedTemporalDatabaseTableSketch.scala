package de.hpi.dataset_versioning.db_synthesis.sketches

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

  override def buildNewTable(unionedTableID: String, unionedTables: mutable.HashSet[DecomposedTemporalTableIdentifier], pkIDSet: collection.Set[Int], newTcSketches: Array[TemporalColumnTrait[Int]]): TemporalDatabaseTableTrait[Int] = {
    new SynthesizedTemporalDatabaseTableSketch(unionedTableID,unionedTables,pkIDSet,newTcSketches.map(tct =>
      tct.asInstanceOf[TemporalColumnSketch]))
      ///new TemporalColumnSketch(unionedTableID,tct.attributeLineage,tct.fieldLineages.map(tft => tft.asInstanceOf[FieldLineageSketch]).toArray)))
  }

  override def informativeTableName: String = getID + "(" + schema.map(_.lastName).mkString(",") + ")"
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