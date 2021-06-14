package de.hpi.tfm.evaluation.data

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.fact_merging.metrics.EdgeScore
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

case class SlimGraph(vertices:Set[String], edges:IndexedSeq[SlimEdge]) extends JsonWritable[SlimGraph]{

  //Tranfer x from scale [a,b] to y in scale [c,d]
  // (x-a) / (b-a) = (y-c) / (d-c)
  //
  //y = (d-c)*(x-a) / (b-a) +c
  def scaleInterpolation(x: Double, a: Double, b: Double, c: Double, d: Double) = {
    val y = (d-c)*(x-a) / (b-a) +c
    assert(y >=c && y <= d)
    y
  }

  def toMDMCPGraph(scoringFunctionThreshold: Double) = {
    val verticesOrdered = vertices.toIndexedSeq.sorted
    val nameToIndexMap = verticesOrdered
      .zipWithIndex
      .toMap
    //Idea: encode doubles as int values in the range
    val scoreRangeIntMin = Integer.MIN_VALUE / 1000.0
    val scoreRangeIntMax = Integer.MAX_VALUE / 1000.0
    val scoreRangeDoubleMin = -1.0
    val scoreRangeDoubleMax = 1.0
    val vertexToEdgesMap = collection.mutable.HashMap[Int,collection.mutable.HashMap[Int,Int]]()
    edges.map(se => {
      val doubleWeightCorrected = se.weight-scoringFunctionThreshold
      if(!(doubleWeightCorrected >= scoreRangeDoubleMin && doubleWeightCorrected <= scoreRangeDoubleMax)){
        println(se)
        println(se.weight)
        println(doubleWeightCorrected)
      }
      assert(doubleWeightCorrected >= scoreRangeDoubleMin && doubleWeightCorrected <= scoreRangeDoubleMax)
      val scoreAsInt = scaleInterpolation(doubleWeightCorrected,scoreRangeDoubleMin,scoreRangeDoubleMax,scoreRangeIntMin,scoreRangeIntMax).round.toInt
      val v1Index = nameToIndexMap(se.id1)
      val v2Index = nameToIndexMap(se.id2)
      assert(v1Index!=v2Index)
      if(v1Index<v2Index)
        vertexToEdgesMap.getOrElseUpdate(v1Index,scala.collection.mutable.HashMap[Int,Int]()).put(v2Index,scoreAsInt)
      else
        vertexToEdgesMap.getOrElseUpdate(v2Index,scala.collection.mutable.HashMap[Int,Int]()).put(v1Index,scoreAsInt)
    })
    MDMCPInputGraph(verticesOrdered,vertexToEdgesMap)
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

object SlimGraph extends JsonReadable[SlimGraph]{

  def fromIdentifiedEdges(edges:collection.Seq[GeneralEdge],scoringFunction:EdgeScore[Any]) = {
    val vertices = edges.flatMap(e => Seq(e.v1.id,e.v2.id)).toSet
    val slimEdges = edges
      .map(e => SlimEdge(e.v1.id,e.v2.id,scoringFunction.compute(e.v1.factLineage.toFactLineage,e.v2.factLineage.toFactLineage)))
      .toIndexedSeq
    SlimGraph(vertices,slimEdges)
  }

}
