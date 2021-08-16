package de.hpi.role_matching.compatibility.graph.representation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.compatibility.graph.representation.slim.SLimGraph
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.io.{File, PrintWriter}

class SubGraph(val graph: Graph[Int, WUnDiEdge]) extends EdgeWeightedSubGraph with StrictLogging{

  def toSerializableComponent = {
    val verticesOrdered = graph.nodes.map(_.value).toIndexedSeq.sorted
    val edges = collection.mutable.HashMap[Int, collection.mutable.HashMap[Int, Float]]()
    graph.edges.foreach(e => {
      assert(e.nodes.size==2)
      val list = e.nodes.toIndexedSeq
      val first = if( list(0).value<list(1).value) list(0).value else list(1).value
      val second = if( list(0).value<list(1).value) list(1).value else list(0).value
      edges.getOrElseUpdate(first, collection.mutable.HashMap[Int, Float]()).put(second,e.weight.toFloat)
    })
    SLimGraph(verticesOrdered.map(_.toString),edges)
  }

  def getEdgeWeight(v: Int, w: Int) = {
    val edges = graph.find(v).get.incoming.filter(_.nodes.exists(_.value==w))
    if(edges.size==0){
      logger.debug("Edge was selected that does not exist - this should never happen")
    }
    assert(edges.size==1)
    edges.head.weight
  }

  def writePartitionVertexFile(file: File) = {
    val pr = new PrintWriter(file)
    graph.nodes.map(_.value).toIndexedSeq.sorted
      .foreach{case (vertex) => pr.println(s"$vertex")}
    pr.close()
  }

  def edgeExists(v: Int, w: Int): Boolean = graph.find(v).get.neighbors.exists(u => u.value==w)

  def nEdges = graph.edges.size


  //defined by the smallest vertex
  def componentName = {
    graph.nodes.map(_.value).min
  }

  def toMDMCPInputFile(f:File) = {
    val verticesOrdered = graph.nodes.map(_.value).toIndexedSeq.sorted
    val pr = new PrintWriter(f)
    pr.println(s" ${verticesOrdered.size}")
    verticesOrdered
      .zipWithIndex
      .foreach{case (v,i) => {
        //val neighbors = adjacencyList.getOrElse(i,Map[Int,Float]())
        val graphNode = graph.find(v).get
        val neighbors = graphNode.incoming
        val neighborsSorted = neighbors
          .toIndexedSeq
          .map(e => (e.nodes.filter(_!=graphNode).head.value,e.weight.toFloat))
          .toMap
        assert(!neighbors.isEmpty)
        val weights = Seq(0) ++ ((i+1) until verticesOrdered.size).map{ j =>
          val w = verticesOrdered(j)
          val weight:Float = neighborsSorted.getOrElse(w,Float.MinValue).toFloat
          getScoreAsInt(weight)
        }
        pr.println(weights.mkString("  "))
      }}
    pr.close()
  }

  def nVertices = graph.nodes.size
}
