package de.hpi.dataset_versioning.db_synthesis.graph.field_lineage

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.AssociationGraphEdge
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.field_graph.BipartiteFieldLineageMatchGraph
import de.hpi.dataset_versioning.io.IOService

/** *
 * Creates edges between two associations
 */
object BipartiteFieldLineageMergeabilityGraphCreationMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val edges = AssociationGraphEdge.fromJsonObjectPerLineFile(args(1))
  for (edge <- edges) {
    logger.debug(s"Discovering mergeability for $edge")
    val tableLeft = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(edge.firstMatchPartner)
    val tableRight = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(edge.secondMatchPartner)
    val leftTableHasChanges = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countChanges(tableLeft)._1 > 0
    val rightTableHasChanges = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countChanges(tableRight)._1 > 0
    if (leftTableHasChanges && rightTableHasChanges) {
      val matchGraph = new BipartiteFieldLineageMatchGraph(tableLeft.tupleReferences, tableRight.tupleReferences)
        .toFieldLineageMergeabilityGraph(true)
      logger.debug(s"Found ${matchGraph.edges.size} edges of which ${matchGraph.edges.filter(_.evidence > 0).size} have more than 0 evidence ")
      if (matchGraph.edges.size > 0) {
        matchGraph.writeToStandardFile()
        val graphRead = FieldLineageMergeabilityGraph.readFromStandardFile(Set(edge.firstMatchPartner, edge.secondMatchPartner))
        assert(graphRead.edges.toSet == matchGraph.edges.toSet)
        //aggregate to associationMatchGraph:
        val tg = matchGraph.transformToTableGraph
        assert(tg.edges.size <= 1)
        if (tg.edges.size > 0) {
          val filename = tg.edges.head.v1.compositeID + ";" + tg.edges.head.v2.compositeID + ".json"
          val subdomain = tg.edges.head.v1.subdomain
          tg.writeToSingleEdgeFile(filename, subdomain)
        }
      }
    }
  }
  //pubx-yq2d.0_17,pubx-yq2d.0_2
}
