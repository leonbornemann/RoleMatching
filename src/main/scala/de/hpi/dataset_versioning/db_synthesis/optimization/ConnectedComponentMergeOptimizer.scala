package de.hpi.dataset_versioning.db_synthesis.optimization

import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.graph.field_lineage.FieldLineageMergeabilityGraph
import scalax.collection.Graph
import scalax.collection.edge.WLkUnDiEdge

import java.io.File
import scala.io.Source

class ConnectedComponentMergeOptimizer(subdomain: String, connectedComponentListFile: File) {

  val inputTables = Source.fromFile(connectedComponentListFile)
    .getLines()
    .toIndexedSeq
    .map(stringID => {
      val id = DecomposedTemporalTableIdentifier.fromCompositeID(stringID)
      val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)
      (id,table)
    }).toMap

  val fieldLineageMergeabilityGraph = FieldLineageMergeabilityGraph.loadSubGraph(inputTables.keySet,subdomain)
  val inputGraph = fieldLineageMergeabilityGraph.transformToOptimizationGraph(inputTables)


  def componentToGraph(e: inputGraph.Component) = {
    val vertices = e.nodes.map(_.value).toSet
    val edges = e.edges.map(e => {
      val nodes = e.toIndexedSeq.map(_.value)
      assert(nodes.size == 2)
      WLkUnDiEdge(nodes(0), nodes(1))(e.weight, e.label)
    })
    val subGraph = Graph.from(vertices, edges)
    subGraph
  }

}


