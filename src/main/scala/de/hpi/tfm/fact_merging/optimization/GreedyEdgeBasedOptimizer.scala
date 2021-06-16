package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.evaluation.data.{ComponentWiseOptimizer, IdentifiedTupleMerge}
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.io.File

class GreedyEdgeBasedOptimizer(graph: Graph[Int, WUnDiEdge],
                               resultFile:File) extends ComponentWiseOptimizer(graph,resultFile) {
  def componentIterator() = new ComponentIterator(graph)


  override def mergeComponent(subGraph: Graph[Int, WUnDiEdge]): Set[IdentifiedTupleMerge] = {
    val componentOptimizer = new GreedyEdgeWeightOptimizerForComponent(subGraph)
    componentOptimizer.mergeGreedily()
  }
}
