package de.hpi.tfm.compatibility

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.tfmp_input.association.AssociationSchema
import de.hpi.tfm.data.tfmp_input.table.sketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG

class AssociationGraphEdgeCandidateGenerator(subdomain: String,
                                             graphConfig: GraphConfig) extends StrictLogging{

  val associationsWithChanges = AssociationSchema.loadAllAssociationsInSubdomain(subdomain)
    .map(as => {
      val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.loadFromFullTimeAssociationSketch(as.id)
      val hasChanges = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countChanges(table)._1 > 0
      val resTable = table.projectToTimeRange(graphConfig.timeRangeStart,graphConfig.timeRangeEnd)
      if(hasChanges) Some(resTable) else None
    })
    .filter(_.isDefined)
    .map(_.get)
    .toSet
  logger.debug(s"Initialized AssociationGraphEdgeCandidateGenerator with ${associationsWithChanges.size} associations containing changes")

  def serializeAllCandidates() = {
    new AssociationEdgeCandidateFinder(associationsWithChanges,graphConfig,subdomain)
  }

  def serializeAllCandidatesNew() = {
    new CompatiblityGraphCreator(associationsWithChanges,subdomain)
  }

}
