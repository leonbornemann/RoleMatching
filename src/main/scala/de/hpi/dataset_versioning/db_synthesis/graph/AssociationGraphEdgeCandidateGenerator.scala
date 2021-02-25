package de.hpi.dataset_versioning.db_synthesis.graph

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{AssociationEdgeCandidateFinder, DataBasedMatchCalculator}
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema

class AssociationGraphEdgeCandidateGenerator(subdomain: String) extends StrictLogging{


  val associationsWithChanges = AssociationSchema.loadAllAssociationsInSubdomain(subdomain)
    .map(as => {
      val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.loadFromStandardOptimizationInputFile(as.id)
      val hasChanges = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countChanges(table)._1 > 0
      if(hasChanges) Some(table) else None
    })
    .filter(_.isDefined)
    .map(_.get)
    .toSet
  logger.debug(s"Initialized AssociationGraphEdgeCandidateGenerator with ${associationsWithChanges.size} associations containing changes")

  def serializeAllCandidates() = {
    new AssociationEdgeCandidateFinder(associationsWithChanges,new DataBasedMatchCalculator())

  }

}
