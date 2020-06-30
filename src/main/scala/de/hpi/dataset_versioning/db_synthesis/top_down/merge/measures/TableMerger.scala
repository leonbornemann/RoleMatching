package de.hpi.dataset_versioning.db_synthesis.top_down.merge.measures

import de.hpi.dataset_versioning.data.simplified.{Attribute, RelationalDataset, RelationalDatasetRow}
import de.hpi.dataset_versioning.db_synthesis.top_down.decomposition.normalization.DecomposedTable

import scala.collection.mutable

class TableMerger() {

  def getProjection(r: RelationalDatasetRow, attributes: collection.IndexedSeq[Attribute]): collection.IndexedSeq[Any] = {
    attributes.map(a => r.fields(a.position.get))
  }

  def mergeDecomposedTables(t1: DecomposedTable, t1Original: RelationalDataset, t2: DecomposedTable, t2Original: RelationalDataset, attributeMapping: Map[Attribute, Attribute]) = {
    assert(t1.version==t2.version)
    val tuppleMapping = mutable.ArrayBuffer[DatasetRowIDMapping]() //together with attribute mapping, this will create the field-mapping function
    val attributeMappingCrossDS = mutable.ArrayBuffer[DatasetAttributeMapping]()
    val mergeResult = RelationalDataset.createEmpty(t1.compositeID + "_" + t2.compositeID,t1.version)
    val sortedAttributesDS1 = attributeMapping.keySet.toIndexedSeq.sortBy(_.position.get)
    mergeResult.attributes = sortedAttributesDS1 //we simply pick attribute order of table 1
    //build column mapping:
    sortedAttributesDS1.foreach(a => attributeMappingCrossDS += DatasetAttributeMapping(t1.originalID,a.position.get,mergeResult.id,a.position.get))
    sortedAttributesDS1.foreach(ds1Attr => {
      val ds2Attr = attributeMapping(ds1Attr)
      attributeMappingCrossDS += DatasetAttributeMapping(t2.originalID,ds2Attr.position.get,mergeResult.id,ds2Attr.position.get)
    })
    val rowSet = mutable.LinkedHashMap[collection.IndexedSeq[Any],RelationalDatasetRow]()
    var curRowId = 0
    t1Original.rows.foreach(r => {
      val newRow = RelationalDatasetRow(curRowId, getProjection(r, mergeResult.attributes))
      if(rowSet.contains(newRow.fields)){
        tuppleMapping += DatasetRowIDMapping(t1.originalID,t1.id,r.id,rowSet(newRow.fields).id)
      } else{
        rowSet.put(newRow.fields,newRow)
        tuppleMapping += DatasetRowIDMapping(t1.originalID,t1.id,r.id,curRowId)
        curRowId+=1
      }
    })
    t2Original.rows.foreach(r => {
      val newRow = RelationalDatasetRow(curRowId, getProjection(r, mergeResult.attributes.map(a => attributeMapping(a))))
      if(rowSet.contains(newRow.fields)){
        tuppleMapping += DatasetRowIDMapping(t2.originalID,t2.id,r.id,rowSet(newRow.fields).id)
      } else{
        rowSet.put(newRow.fields,newRow)
        tuppleMapping += DatasetRowIDMapping(t2.originalID,t2.id,r.id,curRowId)
        curRowId+=1
      }
    })
    mergeResult.rows.addAll(rowSet.valuesIterator)
    (mergeResult,FieldMapping(attributeMappingCrossDS,tuppleMapping)) //TODO: test this code
  }

}
