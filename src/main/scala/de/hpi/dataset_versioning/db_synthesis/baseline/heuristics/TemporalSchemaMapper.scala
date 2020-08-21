package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.SynthesizedTemporalDatabaseTable

import scala.collection.mutable

class TemporalSchemaMapper() {

  def enumerateAllValidSchemaMappings(tableA: SynthesizedTemporalDatabaseTable, tableB: SynthesizedTemporalDatabaseTable):collection.Seq[collection.Map[Set[AttributeLineage],Set[AttributeLineage]]] = {
    assert(tableA.nonKeyAttributeLineages.size==1)
    assert(tableA.nonKeyAttributeLineages.size==1)
    var mapping = mutable.HashMap[Set[AttributeLineage],Set[AttributeLineage]]()
    mapping.put(Set(tableA.nonKeyAttributeLineages.head),Set(tableB.nonKeyAttributeLineages.head))
    if(tableA.nonKeyAttributeLineages.size >1 || tableB.nonKeyAttributeLineages.size>1){
      throw new AssertionError("not yet implemented")
    } else{
      mapping.put(Set(tableA.nonKeyAttributeLineages.head),Set(tableB.nonKeyAttributeLineages.head))
    }
    val a = Seq(mapping)
    a
  }

}
