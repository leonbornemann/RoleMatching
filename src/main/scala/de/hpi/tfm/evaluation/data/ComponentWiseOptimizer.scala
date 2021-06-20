package de.hpi.tfm.evaluation.data

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.evaluation.Histogram
import de.hpi.tfm.fact_merging.optimization.SubGraph
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.io.{File, PrintWriter}

abstract class ComponentWiseOptimizer(val inputGraph: Graph[Int, WUnDiEdge], resultFile:File) extends StrictLogging{

  def componentToGraph(e: inputGraph.Component) = {
    val vertices = e.nodes.map(_.value).toSet
    val edges = e.edges.map(e => {
      val nodes = e.toIndexedSeq.map(_.value)
      assert(nodes.size == 2)
      WUnDiEdge(nodes(0), nodes(1))(e.weight)
    })
    val subGraph = Graph.from(vertices, edges)
    subGraph
  }

  var chosenmerges = scala.collection.mutable.HashSet[IdentifiedTupleMerge]()

  def optimizeComponent(subGraph: SubGraph) :Iterable[IdentifiedTupleMerge]

  def printComponentSizeHistogram() = {
    val traverser = inputGraph.componentTraverser()
    val sizes = traverser.toIndexedSeq.map(e => {
      val subGraph: Graph[Int, WUnDiEdge] = componentToGraph(e)
      (subGraph.nodes.size,subGraph.edges.size)
    })
    val nodeCountHistogram = Histogram(sizes.map(_._1))
    nodeCountHistogram.printAll()
  }

  def runComponentWiseOptimization() = {
    logger.debug(s"Starting Clique Partitioning Optimization")
    val numCompontents = inputGraph.componentTraverser().size
    logger.debug(s"Input Graph has ${inputGraph.nodes.size} vertices and ${inputGraph.edges.size} edges and $numCompontents connected components")
    val traverser = inputGraph.componentTraverser()
    val pr = new PrintWriter(resultFile)
    var i = 0
    traverser.foreach(e => {
      val subGraph = new SubGraph(componentToGraph(e))
      //logger.debug(s"Handling Component with Vertices: ${subGraph.nodes.map(_.value)}")
      //logger.debug(s"Vertex Count: ${subGraph.nodes.size}, edge count: ${subGraph.edges.size}")
      val componentMerges = optimizeComponent(subGraph)
      if(!(componentMerges.toIndexedSeq.flatMap(_.clique).size==subGraph.nVertices)) {
        println()
      }
      assert(componentMerges.toIndexedSeq.flatMap(_.clique).size==subGraph.nVertices)
      componentMerges.foreach(tm => {
        tm.appendToWriter(pr,false,true)
      })
      i+=1
      if(i%1000==0) {
        logger.debug(s"Finished $i connected components (${100*i/numCompontents.toDouble}%)")
      }
    })
    pr.close()
  }



}
