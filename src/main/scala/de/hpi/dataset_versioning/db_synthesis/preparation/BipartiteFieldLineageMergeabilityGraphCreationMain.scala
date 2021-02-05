package de.hpi.dataset_versioning.db_synthesis.preparation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{AssociationGraphEdge, BipartiteFieldLineageMatchGraph}
import de.hpi.dataset_versioning.db_synthesis.preparation.InternalFieldLineageMatchGraphCreationMain.args
import de.hpi.dataset_versioning.io.IOService


/***
 * Creates edges between two associations
 */
object BipartiteFieldLineageMergeabilityGraphCreationMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val edges = AssociationGraphEdge.fromJsonObjectPerLineFile(args(1))
  for(edge <- edges){
    logger.debug(s"Discovering mergeability for $edge")
    val tableLeft = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(edge.firstMatchPartner)
    val tableRight = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(edge.secondMatchPartner)
    val leftTableHasChanges = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(tableLeft)._1 > 0
    val rightTableHasChanges = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(tableRight)._1 > 0
    if(leftTableHasChanges && rightTableHasChanges){
      val matchGraph = new BipartiteFieldLineageMatchGraph(tableLeft.tupleReferences,tableRight.tupleReferences)
        .toFieldLineageMergeabilityGraph
      logger.debug(s"Found ${matchGraph.edges.size} edges of which ${matchGraph.edges.filter(_.evidence>0).size} have more than 0 evidence ")
      if(matchGraph.edges.size>0)
        matchGraph.writeToStandardFile()
    }
  }

}
