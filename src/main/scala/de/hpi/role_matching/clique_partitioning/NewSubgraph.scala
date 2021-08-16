package de.hpi.role_matching.clique_partitioning

import de.hpi.role_matching.compatibility.graph.representation.EdgeWeightedSubGraph
import org.jgrapht.{Graph, Graphs}
import org.jgrapht.graph.{AsSubgraph, DefaultWeightedEdge}

import java.io.{File, PrintWriter}
import scala.jdk.CollectionConverters.{ListHasAsScala, SetHasAsScala}

class NewSubgraph(val graph: Graph[Int, DefaultWeightedEdge]) extends EdgeWeightedSubGraph{
  def getEdgeWeight(v: Int, w: Int) = graph.getEdgeWeight(graph.getEdge(v,w))

  def neighborsOf(head: Int) = Graphs.neighborListOf(graph,head).asScala.toSet

  def nEdges = graph.edgeSet().size()


  def writePartitionVertexFile(f: File): Unit = {
    val pr = new PrintWriter(f)
    graph.vertexSet().asScala.toIndexedSeq.sorted
      .foreach{case (vertex) => pr.println(s"$vertex")}
    pr.close()
  }

  def toMDMCPInputFile(f: File) = {
    val verticesOrdered = graph.vertexSet().asScala.toIndexedSeq.sorted
    val pr = new PrintWriter(f)
    pr.println(s" ${verticesOrdered.size}")
    verticesOrdered
      .zipWithIndex
      .foreach{case (v,i) => {
        //val neighbors = adjacencyList.getOrElse(i,Map[Int,Float]())

        val neighbours = Graphs.neighborListOf(graph,v).asScala
          .map(w => (w,graph.getEdgeWeight(graph.getEdge(v,w))))
          .toMap
        assert(!neighbours.isEmpty)
        val weights = Seq(0) ++ ((i+1) until verticesOrdered.size).map{ j =>
          val w = verticesOrdered(j)
          val weight:Double = neighbours.getOrElse(w,Double.MinValue)
          getScoreAsInt(weight)
        }
        pr.println(weights.mkString("  "))
      }}
    pr.close()
  }

  def nVertices = graph.vertexSet().size()

  def componentName = {
    val s = graph.vertexSet().asScala.min
  }

}
