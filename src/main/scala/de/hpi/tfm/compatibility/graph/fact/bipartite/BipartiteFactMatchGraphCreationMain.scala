package de.hpi.tfm.compatibility.graph.fact.bipartite

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.graph.association.AssociationGraphEdgeCandidate
import de.hpi.tfm.compatibility.graph.fact.FactMergeabilityGraph
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.IOService

/** *
 * Creates edges between two associations
 */
object BipartiteFactMatchGraphCreationMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val edges = AssociationGraphEdgeCandidate.fromJsonObjectPerLineFile(args(1))
  val tables = scala.collection.mutable.HashMap[AssociationIdentifier,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]()
  val minEvidence = args(2).toInt

  def getOrLoad(id: AssociationIdentifier) = {
    tables.getOrElseUpdate(id,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id))
  }

  for (edge <- edges) {
    logger.debug(s"Discovering mergeability for $edge")
    val tableLeft = getOrLoad(edge.firstMatchPartner)
    val tableRight = getOrLoad(edge.secondMatchPartner)
    val matchGraphEdges = new BipartiteFactMatchCreator(tableLeft.tupleReferences, tableRight.tupleReferences)
      .toFieldLineageMergeabilityGraph(true)
      .edges
      .filter(_.evidence>=minEvidence)
    val matchGraph = FactMergeabilityGraph(matchGraphEdges)
    logger.debug(s"Found ${matchGraph.edges.size} edges ")
    if (matchGraph.edges.size > 0) {
      matchGraph.writeToStandardFile()
      val tg = matchGraph.transformToTableGraph
      assert(tg.edges.size <= 1)
      if (tg.edges.size > 0) {
        val filename = tg.edges.head.v1.compositeID + ";" + tg.edges.head.v2.compositeID + ".json"
        val subdomain = tg.edges.head.v1.subdomain
        tg.writeToSingleEdgeFile(filename, subdomain)
      }
    }
  }
  //pubx-yq2d.0_17,pubx-yq2d.0_2
}
