package de.hpi.tfm.evaluation.data

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.fact_merging.metrics.EdgeScore
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

case class SlimGraphOld(vertices:Set[String], edges:IndexedSeq[SlimEdge]) extends JsonWritable[SlimGraphOld]{

  //Tranfer x from scale [a,b] to y in scale [c,d]
  // (x-a) / (b-a) = (y-c) / (d-c)
  //
  //y = (d-c)*(x-a) / (b-a) +c
  def scaleInterpolation(x: Double, a: Double, b: Double, c: Double, d: Double) = {
    val y = (d-c)*(x-a) / (b-a) +c
    assert(y >=c && y <= d)
    y
  }


  def transformToOptimizationGraph = {
    val newVertices = scala.collection.mutable.HashSet[String]() ++ vertices
    val newEdges = edges.map(e => {
      WUnDiEdge(e.id1,e.id2)(e.weight)
    })
    val graph = Graph.from(newVertices,newEdges)
    graph
  }

}

object SlimGraphOld extends JsonReadable[SlimGraphOld]{

  def fromIdentifiedEdges(edges:collection.Seq[GeneralEdge],scoringFunction:EdgeScore[Any]) = {
    val vertices = edges.flatMap(e => Seq(e.v1.id,e.v2.id)).toSet
    val slimEdges = edges
      .map(e => SlimEdge(e.v1.id,e.v2.id,scoringFunction.compute(e.v1.factLineage.toFactLineage,e.v2.factLineage.toFactLineage)))
      .toIndexedSeq
    SlimGraphOld(vertices,slimEdges)
  }

}
