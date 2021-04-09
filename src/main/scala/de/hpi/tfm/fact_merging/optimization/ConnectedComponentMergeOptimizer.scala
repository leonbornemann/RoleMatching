package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.FactMergeabilityGraph
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import scalax.collection.Graph
import scalax.collection.edge.WLkUnDiEdge

import java.io.File
import scala.io.Source

abstract class ConnectedComponentMergeOptimizer(subdomain: String, connectedComponentListFile: File,graphConfig: GraphConfig) {

  val inputTables = Source.fromFile(connectedComponentListFile)
    .getLines()
    .toIndexedSeq
    .map(stringID => {
      val id = AssociationIdentifier.fromCompositeID(stringID)
      val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)
      (id,table)
    }).toMap

  val fieldLineageMergeabilityGraph = FactMergeabilityGraph.loadSubGraph(inputTables.keySet,subdomain,graphConfig)
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

  def run():Unit

  def name:String

}


