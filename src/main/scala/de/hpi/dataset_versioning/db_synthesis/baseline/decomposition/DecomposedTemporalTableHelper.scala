package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

import java.time.LocalDate

import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.AttributeLineageWithHashMap
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

case class DecomposedTemporalTableHelper(subdomain: String, originalID: String, id: Int, attributes: collection.IndexedSeq[AttributeLineageWithHashMap], originalFDLHS: collection.Set[AttributeLineageWithHashMap], primaryKeyByVersion: Map[LocalDate,collection.Set[Attribute]])  extends JsonWritable[DecomposedTemporalTableHelper]{

  def toDecomposedTemporalTable = DecomposedTemporalTable(subdomain,originalID,id,attributes.map(_.toDecomposedTemporalTable),
    originalFDLHS.map(_.toDecomposedTemporalTable),
    primaryKeyByVersion)
}

object DecomposedTemporalTableHelper extends JsonReadable[DecomposedTemporalTableHelper]
