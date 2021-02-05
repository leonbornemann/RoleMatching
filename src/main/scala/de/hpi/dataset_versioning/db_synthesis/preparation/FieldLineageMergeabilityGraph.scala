package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.General_1_to_1_TupleMatching
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

case class FieldLineageMergeabilityGraph(edges: IndexedSeq[FieldLineageGraphEdge]) extends JsonWritable[FieldLineageMergeabilityGraph]{

  def idSet:Set[DecomposedTemporalTableIdentifier] = edges.flatMap(e => Set[DecomposedTemporalTableIdentifier](e.tupleReferenceA.associationID,e.tupleReferenceB.associationID)).toSet

  def writeToStandardFile() = {
    toJsonFile(DBSynthesis_IOService.getInternalFieldLineageEdgeFile(idSet))
  }

}
object FieldLineageMergeabilityGraph extends JsonReadable[FieldLineageMergeabilityGraph]{

  def readFromStandardFile(ids:Set[DecomposedTemporalTableIdentifier]) = {
    fromJsonFile(DBSynthesis_IOService.getInternalFieldLineageEdgeFile(ids).getAbsolutePath)
  }

}
