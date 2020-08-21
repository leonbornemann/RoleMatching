package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, TemporalRow, TemporalTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, DecomposedTemporalTableIdentifier}
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage

import scala.collection.mutable

class SynthesizedTemporalDatabaseTable(val unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                       val schema: collection.IndexedSeq[AttributeLineage],
                                       val keyAttributeLineages: collection.Set[AttributeLineage],
                                       private val rows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer(),
                                       private var curEntityIDCounter:Long) {

  def computeUnionMatch(other:SynthesizedTemporalDatabaseTable): Option[ComputedMatch] = {
    //TODO: 1.load all data
    // 2. get best schema mapping given the data
    // 3. return that mapping
    ???
  }


  def getActiveTime = {
    schema.map(_.activeTimeIntervals).reduce((a,b) => a.union(b))
  }

  def nonKeyAttributeLineages = schema.filter(al => !keyAttributeLineages.contains(al))


  var keyIsArtificial = false

  private var fieldAndRowMappings = mutable.HashMap[DecomposedTemporalTable,FieldAndRowMapping]()

  def getBestMergeMapping(curCandidate: DecomposedTemporalTable) = {
    val greedyMatcher = new SuperSimpleGreedySchemaMatcher(this,curCandidate)
    greedyMatcher.getBestSchemaMatching()
  }


}
object SynthesizedTemporalDatabaseTable{

  def initFrom(dttToMerge: DecomposedTemporalTable) = {
    val synthesizedSchema = dttToMerge.containedAttrLineages
    var curEntityID:Long = 0
    val entityIDMatchingSynthesizedToOriginal = mutable.HashMap[Long,Long]()
    val tt = TemporalTable.loadAndCache(dttToMerge.id.viewID) //TODO: we will need to shrink this cache at some point
      .project(dttToMerge)
    val newRows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer()
    //create new temporal rows with new entity ids:
    tt.rows.foreach(tr => {
      newRows.addOne(new TemporalRow(curEntityID,tr.fields))
      entityIDMatchingSynthesizedToOriginal.put(curEntityID,tr.entityID)
      curEntityID +=1
    })
    val attributeMatchingSynthesizedToOriginal = synthesizedSchema.map(al => (al.attrId,al.attrId))
    val synthTable = new SynthesizedTemporalDatabaseTable(mutable.HashSet(dttToMerge.id),
      synthesizedSchema,
      dttToMerge.primaryKey,
      newRows,
      curEntityID)
    synthTable.fieldAndRowMappings.put(dttToMerge,new FieldAndRowMapping(attributeMatchingSynthesizedToOriginal,entityIDMatchingSynthesizedToOriginal))
    synthTable
  }

}