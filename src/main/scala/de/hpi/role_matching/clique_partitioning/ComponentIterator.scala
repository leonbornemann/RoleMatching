package de.hpi.role_matching.clique_partitioning

import de.hpi.role_matching.compatibility.graph.representation.SubGraph
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

class ComponentIterator(val graph: Graph[Int, WUnDiEdge]) extends Iterator[SubGraph]{

  def componentToGraph(e: graph.Component) = {
    val vertices = e.nodes.map(_.value).toSet
    val edges = e.edges.map(e => {
      val nodes = e.toIndexedSeq.map(_.value)
      assert(nodes.size == 2)
      WUnDiEdge(nodes(0), nodes(1))(e.weight)
    })
    val subGraph = Graph.from(vertices, edges)
    subGraph
  }

  val traverser = graph.componentTraverser().iterator

  override def hasNext: Boolean = traverser.hasNext

  override def next(): SubGraph = new SubGraph(componentToGraph(traverser.next()))
}
