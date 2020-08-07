package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.{AttributeLineage, TemporalRow, TemporalTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage

import scala.collection.mutable

class SynthesizedTemporalDatabaseTable(val subdomain:String,
                                       val compositeIDs:mutable.HashSet[String],
                                       val schema: collection.IndexedSeq[AttributeLineage],
                                       private val rows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer(),
                                       private var curEntityIDCounter:Long) {

  private var fieldAndRowMappings = mutable.HashMap[DecomposedTemporalTable,FieldAndRowMapping]()

  def getBestMerge(curCandidate: DecomposedTemporalTable) = {
    val greedyMatcher = new SuperSimpleGreedySchemaMatcher(this,curCandidate)
    greedyMatcher.getBestSchemaMatching()
  }

}
object SynthesizedTemporalDatabaseTable{

  def initFrom(dttToMerge: DecomposedTemporalTable) = {
    val synthesizedSchema = dttToMerge.containedAttrLineages
    var curEntityID:Long = 0
    val entityIDMatchingSynthesizedToOriginal = mutable.HashMap[Long,Long]()
    val tt = TemporalTable.loadAndCache(dttToMerge.originalID) //TODO: we will need to shrink this cache at some point
      .project(dttToMerge)
    val newRows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer()
    //create new temporal rows with new entity ids:
    tt.rows.foreach(tr => {
      newRows.addOne(new TemporalRow(curEntityID,tr.fields))
      entityIDMatchingSynthesizedToOriginal.put(curEntityID,tr.entityID)
      curEntityID +=1
    })
    val attributeMatchingSynthesizedToOriginal = synthesizedSchema.map(al => (al.attrId,al.attrId))
    val synthTable = new SynthesizedTemporalDatabaseTable(dttToMerge.subdomain,
      mutable.HashSet(dttToMerge.compositeID),
      synthesizedSchema,
      newRows,
      curEntityID)
    synthTable.fieldAndRowMappings.put(dttToMerge,new FieldAndRowMapping(attributeMatchingSynthesizedToOriginal,entityIDMatchingSynthesizedToOriginal))
    synthTable
  }

}