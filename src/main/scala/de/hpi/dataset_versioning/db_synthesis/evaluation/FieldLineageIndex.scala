package de.hpi.dataset_versioning.db_synthesis.evaluation

import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.data.change.{Change, ChangeCube}

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class FieldLineageIndex(value: ArrayBuffer[ChangeCube]) {

  def getFieldLineage(tableID: String, attrID: Int, entityID: Int) = {
    index(tableID)((entityID,attrID))
  }

  def toMap(index: ArrayBuffer[Change]) = index
    .groupBy(c => (c.e,c.pID))
    .map{case (id,changes) => (id,ValueLineage(scala.collection.mutable.TreeMap[LocalDate,Any]() ++ changes.map(c => (c.t,c.value))))}

  val index = value
    .map(cc => (cc.datasetID,toMap(cc.allChanges)))
    .toMap
}
