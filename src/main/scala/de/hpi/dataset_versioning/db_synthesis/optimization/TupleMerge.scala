package de.hpi.dataset_versioning.db_synthesis.optimization

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{IDBasedTupleReference, TupleReference}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService.{FIELD_MERGE_RESULT_DIR, createParentDirs}

import java.io.File

case class TupleMerge(clique:Set[IDBasedTupleReference],score:Double) extends JsonWritable[TupleMerge]{

}

object TupleMerge extends JsonReadable[TupleMerge] {

  def getStandardJsonObjectPerLineFile(componentFileName: String) = {
    createParentDirs(new File(FIELD_MERGE_RESULT_DIR + "/" + componentFileName + ".json"))
  }
}
