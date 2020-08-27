package de.hpi.dataset_versioning.db_synthesis.sketches

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, DecomposedTemporalTableIdentifier}
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.TemporalDatabaseTableTrait

import scala.collection.mutable

class SynthesizedTemporalDatabaseTableSketch(val id:String,
                                             val unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                             pkIDs: collection.Set[Int],
                                             temporalColumnSketches:Array[TemporalColumnSketch]) extends TemporalTableSketch(temporalColumnSketches) with TemporalDatabaseTableTrait{

  def primaryKey = schema.filter(al => pkIDs.contains(al.attrId)).toSet

  def schema = temporalColumnSketches.map(tc => tc.attributeLineage).toIndexedSeq

  def nonKeyAttributeLineages = schema.filter(al => !primaryKey.contains(al))

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