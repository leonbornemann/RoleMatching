package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.oneshot.DBSIIExerciseExport.args

object ScriptMain2 extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val id = "4n2t-us8h"
  val bcnfID = 4
  val assocaitionID = Some(0)
  val rowIndex = 0
  val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
    .loadFromStandardOptimizationInputFile(DecomposedTemporalTableIdentifier(subdomain,id,bcnfID,assocaitionID))
  val a = table.getDataTuple(rowIndex)
  println(a)

}
