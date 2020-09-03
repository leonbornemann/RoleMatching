package de.hpi.dataset_versioning.db_synthesis.top_down_no_change.merge.measures

import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.database.SynthesizedDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization.DecomposedTable
import de.hpi.dataset_versioning.io.IOService

class InferredChangeCountReduction(countInitialInserts:Boolean = true) extends TableMergeMeasure {

  val tableMerger = new TableMerger()

  /***
   * This only works if we have a perfect mapping from t1 to t2!
   * @param t1
   * @param t2
   * @param mapping
   * @return
   */
  override def calculate(t1: DecomposedTable, t2: DecomposedTable, mapping: Map[Attribute, Attribute]): Int = {
    if(!countInitialInserts)
      throw new AssertionError("not yet implemented")
    else{
      val t1Original = IOCache.getOrLoadOriginalDataset(t1)
      val t2Original = IOCache.getOrLoadOriginalDataset(t2)
      val t1AttrSet = t1.attributes.map(f => f.id).toSet
      val t2AttrSet = t2.attributes.map(f => f.id).toSet
      val changesT1 = IOCache.getOrLoadChanges(t1.originalID)
        .filterChangesInPlace(c => t1AttrSet.contains(c.pID))
      val changesT2 = IOCache.getOrLoadChanges(t2.originalID)
        .filterChangesInPlace(c => t2AttrSet.contains(c.pID))
      val (mergedTable,fieldMapping) = tableMerger.mergeDecomposedTables(t1,t1Original,t2,t2Original,mapping)
      val processedMappedIDs = collection.mutable.HashSet[Long]()
      val originalChanges = changesT1.changeCount(countInitialInserts) + changesT2.changeCount(countInitialInserts)
      //map by entity
      val t1ChangesByEntity = changesT1.allChanges.groupBy(_.e)
        .map{case (k,v) => (k,v.groupBy(_.pID))}
      val t2ChangesByEntity = changesT2.allChanges.groupBy(_.e)
        .map{case (k,v) => (k,v.groupBy(_.pID))}
      var changesInMergedDs = 0
      fieldMapping.tupleMappings.foreach(tm => {
        val originalRow = tm.originalRowID
        val mappedRow = tm.mappedRowID
        if(!processedMappedIDs.contains(mappedRow)){
          val tupleChangesByProperty = if(tm.dsID==t1.originalID) t1ChangesByEntity(originalRow) else t2ChangesByEntity(originalRow)
          val decomposedTableAttributeIDs = if(tm.dsID==t1.originalID) t1AttrSet else t2AttrSet
          if(countInitialInserts) {
            changesInMergedDs += tupleChangesByProperty.map(_._2.size).sum//decomposedTableAttributeIDs.map(id => tupleChangesByProperty.getOrElse(id,Seq()).size).sum
          }
          else
            changesInMergedDs += tupleChangesByProperty.map(_._2.size).sum //decomposedTableAttributeIDs.map(id => tupleChangesByProperty(id).size).sum - decomposedTableAttributeIDs.size
          processedMappedIDs += mappedRow
        }
      })
      originalChanges - changesInMergedDs
    }
  }

  override def calculate(synthTable: SynthesizedDatabaseTable, t: DecomposedTable, mapping: Map[Attribute, Attribute]): Int = {
    ???
  }
}
