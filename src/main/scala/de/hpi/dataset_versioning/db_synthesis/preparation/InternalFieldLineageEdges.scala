package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.General_1_to_1_TupleMatching
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

case class InternalFieldLineageEdges(id: DecomposedTemporalTableIdentifier, toIndexedSeq: IndexedSeq[General_1_to_1_TupleMatching[Any]]) extends JsonWritable[InternalFieldLineageEdges]{

  def writeToStandardFile() = {
    toJsonFile(DBSynthesis_IOService.getInternalFieldLineageEdgeFile(id))
  }

}
object InternalFieldLineageEdges extends JsonReadable[InternalFieldLineageEdges]{

  def readFromStandardFile(id:DecomposedTemporalTableIdentifier) = {
    fromJsonFile(DBSynthesis_IOService.getInternalFieldLineageEdgeFile(id).getAbsolutePath)
  }

}
