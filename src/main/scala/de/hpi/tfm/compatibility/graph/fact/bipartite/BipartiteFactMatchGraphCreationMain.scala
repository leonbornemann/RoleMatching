package de.hpi.tfm.compatibility.graph.fact.bipartite

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.graph.association.AssociationGraphEdgeCandidate
import de.hpi.tfm.compatibility.graph.fact.FactMergeabilityGraph
import de.hpi.tfm.data.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.IOService

/** *
 * Creates edges between two associations
 */
object BipartiteFactMatchGraphCreationMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val edges = AssociationGraphEdgeCandidate.fromJsonObjectPerLineFile(args(1))
  val minEvidence = args(2).toInt
  for (edge <- edges) {
    logger.debug(s"Discovering mergeability for $edge")
    val tableLeft = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(edge.firstMatchPartner)
    val tableRight = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(edge.secondMatchPartner)
    val leftTableHasChanges = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countChanges(tableLeft)._1 > 0
    val rightTableHasChanges = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countChanges(tableRight)._1 > 0
    if (leftTableHasChanges && rightTableHasChanges) {
      val matchGraphEdges = new BipartiteFactMatchCreator(tableLeft.tupleReferences, tableRight.tupleReferences)
        .toFieldLineageMergeabilityGraph(true)
        .edges
        .filter(_.evidence>=minEvidence)
      val matchGraph = FactMergeabilityGraph(matchGraphEdges)
      logger.debug(s"Found ${matchGraph.edges.size} edges of which ${matchGraph.edges.filter(_.evidence > 0).size} have more than 0 evidence ")
      logger.debug("----------------------------------------------------------------------------------------------------------------------------------------------")
      logger.debug("----------------------------------------------------------------------------------------------------------------------------------------------")
      if (matchGraph.edges.size > 0) {
        matchGraph.writeToStandardFile()
        val graphRead = FactMergeabilityGraph.readFromStandardFile(Set(edge.firstMatchPartner, edge.secondMatchPartner))
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
