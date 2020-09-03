package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.SynthesizedTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch

import scala.collection.mutable

class TemporalSchemaMapper() extends StrictLogging{

  def enumerateAllValidSchemaMappings[A](tableA: TemporalDatabaseTableTrait[A], tableB: TemporalDatabaseTableTrait[A]):collection.Seq[collection.Map[Set[AttributeLineage],Set[AttributeLineage]]] = {
    assert(tableA.nonKeyAttributeLineages.size==1)
    assert(tableA.nonKeyAttributeLineages.size==1)
    val mapping = mutable.HashMap[Set[AttributeLineage],Set[AttributeLineage]]()
    mapping.put(Set(tableA.nonKeyAttributeLineages.head),Set(tableB.nonKeyAttributeLineages.head))
    if(tableA.nonKeyAttributeLineages.size >1 || tableB.nonKeyAttributeLineages.size>1){
      throw new AssertionError("not yet implemented")
    } else{
      mapping.put(Set(tableA.nonKeyAttributeLineages.head),Set(tableB.nonKeyAttributeLineages.head))
      mapping.put(Set(tableA.primaryKey.head),Set(tableB.primaryKey.head))
    }
    val a = Seq(mapping)
    a
  }
}
object TemporalSchemaMapper extends StrictLogging{
  logger.debug("TemporalSchemaMapper currently only supports very simple schema-mapping restricted to dtts with schema size 2 - this will have to be improved")

}
