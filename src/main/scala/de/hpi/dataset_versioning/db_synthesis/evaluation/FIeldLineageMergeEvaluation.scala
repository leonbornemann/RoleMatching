package de.hpi.dataset_versioning.db_synthesis.evaluation

import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.optimization.TupleMerge
import de.hpi.dataset_versioning.io.IOService

object FieldLineageMergeEvaluation extends App {
  IOService.socrataDir = args(0)
  val files = TupleMerge.getStandardObjectPerLineFiles
  for(file <- files){
    val merges = TupleMerge.fromJsonObjectPerLineFile(file.getAbsolutePath)
    val tables = merges.flatMap(_.clique.map(_.associationID).toSet).toSet
    val byID = tables.map(id => (id,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)))
      .toMap

  }
}
