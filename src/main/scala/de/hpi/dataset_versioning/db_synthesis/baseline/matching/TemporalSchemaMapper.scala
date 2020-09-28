package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait

import scala.collection.mutable

class TemporalSchemaMapper() extends StrictLogging{

  def enumerateAllValidSchemaMappings[A](tableA: TemporalDatabaseTableTrait[A], tableB: TemporalDatabaseTableTrait[A]):collection.Seq[collection.Map[Set[AttributeLineage],Set[AttributeLineage]]] = {
    assert(tableA.nonKeyAttributeLineages.size==1)
    assert(tableA.nonKeyAttributeLineages.size==1)
    val allMappings = mutable.ArrayBuffer[collection.Map[Set[AttributeLineage],Set[AttributeLineage]]]()
    if(tableA.nonKeyAttributeLineages.size >1 || tableB.nonKeyAttributeLineages.size>1){
      throw new AssertionError("not yet implemented")
    } else if(tableA.primaryKey.size == 1 && tableB.primaryKey.size==1) {
      val mapping = mutable.HashMap[Set[AttributeLineage],Set[AttributeLineage]]()
      mapping.put(Set(tableA.nonKeyAttributeLineages.head),Set(tableB.nonKeyAttributeLineages.head))
      mapping.put(Set(tableA.primaryKey.head),Set(tableB.primaryKey.head))
      allMappings.addOne(mapping)
    } else{
      val colCombinationEnumerator = new LatticeBasedInterTableColumnMergeEnumerator()
      //TODO: Test this!
      val allValidAttributeCombinationsA = colCombinationEnumerator.enumerateAll(tableA.primaryKey.toIndexedSeq)
      val allValidAttributeCombinationsB = colCombinationEnumerator.enumerateAll(tableB.primaryKey.toIndexedSeq)
      if(allValidAttributeCombinationsA.size==1 && allValidAttributeCombinationsB.contains(tableB.primaryKey) ||
        allValidAttributeCombinationsB.size==1 && allValidAttributeCombinationsA.contains(tableA.primaryKey)){
        val mapping = mutable.HashMap[Set[AttributeLineage],Set[AttributeLineage]]()
        mapping.put(Set(tableA.nonKeyAttributeLineages.head),Set(tableB.nonKeyAttributeLineages.head))
        mapping.put(Set() ++ tableA.primaryKey,Set() ++ tableB.primaryKey)
        allMappings.addOne(mapping)
      } else {
        ??? //TODO: implement this in a reasonable way
      }
    }
    allMappings
  }
}
object TemporalSchemaMapper extends StrictLogging{
  logger.debug("TemporalSchemaMapper currently only supports very simple schema-mapping restricted to dtts with schema size 2 - this will have to be improved")

}
