package de.hpi.role_matching.cbrm.sgcp

import de.hpi.role_matching.cbrm.compatibility_graph.representation.EdgeWeightedSubGraph
import org.jgrapht.graph.DefaultWeightedEdge
import org.jgrapht.{Graph, Graphs}

import java.io.{File, PrintWriter}
import scala.jdk.CollectionConverters.{ListHasAsScala, SetHasAsScala}

class NewSubgraph(val graph: Graph[Int, DefaultWeightedEdge]) extends EdgeWeightedSubGraph{
  def getEdgeWeight(v: Int, w: Int) = {
    val e = graph.getEdge(v,w)
    if(e==null)
      println()
    graph.getEdgeWeight(graph.getEdge(v,w))
  }

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
    val edgeIterator = graph.edgeSet().iterator()
    var min = Double.MaxValue
    var max =Double.MinValue
    while(edgeIterator.hasNext){
      val curEdge = edgeIterator.next()
      val curEdgeWeight = graph.getEdgeWeight(curEdge)
      if(curEdgeWeight<min)
        min = curEdgeWeight-1
      if(curEdgeWeight>max)
        max = curEdgeWeight+1
    }
    scoreRangeDoubleMin=min.toFloat
    scoreRangeDoubleMax=max.toFloat
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
          val newWeight = getScoreAsInt(weight)
          newWeight
        }
        pr.println(weights.mkString("  "))
      }}
    pr.close()
  }

  def nVertices = graph.vertexSet().size()

  def componentName = {
    val s = graph.vertexSet().asScala.min
    s
  }

}
