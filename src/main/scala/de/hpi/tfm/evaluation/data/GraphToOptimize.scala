package de.hpi.tfm.evaluation.data

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.fact_merging.metrics.EdgeScore
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

case class SlimOptimizationGraph(vertices:Set[String],edges:IndexedSeq[SlimEdge]) extends JsonWritable[SlimOptimizationGraph]{

  def transformToOptimizationGraph = {
    val newVertices = scala.collection.mutable.HashSet[String]() ++ vertices
    val newEdges = edges.map(e => {
      WUnDiEdge(e.id1,e.id2)(e.weight)
    })
    val graph = Graph.from(newVertices,newEdges)
    graph
  }

}

object SlimOptimizationGraph extends JsonReadable[SlimOptimizationGraph]{

  def fromIdentifiedEdges(edges:IndexedSeq[GeneralEdge],scoringFunction:EdgeScore[Any]) = {
    val vertices = edges.flatMap(e => Seq(e.v1.id,e.v2.id)).toSet
    val slimEdges = edges.map(e => SlimEdge(e.v1.id,e.v2.id,scoringFunction.compute(e.v1.factLineage.toFactLineage,e.v2.factLineage.toFactLineage)))
    SlimOptimizationGraph(vertices,slimEdges)
  }

}
