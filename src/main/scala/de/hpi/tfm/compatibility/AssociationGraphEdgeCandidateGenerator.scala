package de.hpi.tfm.compatibility

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.tfmp_input.association.AssociationSchema
import de.hpi.tfm.data.tfmp_input.table.sketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG

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
    new AssociationEdgeCandidateFinder(associationsWithChanges)
  }

  def serializeAllCandidatesNew() = {
    new CompatiblityGraphCreator(associationsWithChanges,subdomain)
  }

}
