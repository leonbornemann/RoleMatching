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
    val rows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer()
    val synthesizedSchema = dttToMerge.containedAttrLineages
    var curEntityID:Long = 0
    val entityIDMatchingSynthesizedToOriginal = mutable.HashMap[Long,Long]()
    val tt = TemporalTable.load(dttToMerge.originalID)
      //.project(dttToMerge)
    val alIDToPosInNewTable = dttToMerge.containedAttrLineages.zipWithIndex.map{case (al,i) => (al.attrId,i)}.toMap
    val oldAttributePositionToNewAttributePosition = tt.attributes.zipWithIndex
      .withFilter(al => alIDToPosInNewTable.contains(al._1.attrId))
      .map{case (al,i) => (i,alIDToPosInNewTable(al.attrId))}.toIndexedSeq
    tt.rows.foreach(tr => {
      val newRowContent = oldAttributePositionToNewAttributePosition.map{case (oldIndex,newIndex) => {
        (newIndex,tr.fields(oldIndex))
      }}.sortBy(_._1)
      assert(newRowContent.map(_._1) == (0 until synthesizedSchema.size))
      val newRow = new TemporalRow(curEntityID,newRowContent.map(_._2))
      entityIDMatchingSynthesizedToOriginal.put(curEntityID,tr.entityID)
      curEntityID +=1
      rows.addOne(newRow)
    })
    val attributeMatchingSynthesizedToOriginal = synthesizedSchema.map(al => (al.attrId,al.attrId))
    val synthTable = new SynthesizedTemporalDatabaseTable(dttToMerge.subdomain,
      mutable.HashSet(dttToMerge.compositeID),
      synthesizedSchema,
      rows,
      curEntityID)
    synthTable.fieldAndRowMappings.put(dttToMerge,new FieldAndRowMapping(attributeMatchingSynthesizedToOriginal,entityIDMatchingSynthesizedToOriginal))
    synthTable
  }

}