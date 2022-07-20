package de.hpi.role_matching.cbrm.sgcp

import com.typesafe.scalalogging.StrictLogging
import org.jgrapht.Graph
import org.jgrapht.alg.connectivity.ConnectivityInspector
import org.jgrapht.graph.{AsSubgraph, DefaultWeightedEdge}

import java.io.File

abstract class ComponentWiseOptimizer(val inputGraph: Graph[Int, DefaultWeightedEdge], resultDir:File) extends StrictLogging{

  var chosenmerges = scala.collection.mutable.HashSet[RoleMerge]()

  def optimizeComponent(subGraph:NewSubgraph)

  def closeAllWriters()

  def getComponents(inputGraph: Graph[Int, DefaultWeightedEdge]) = {
    val sets = new ConnectivityInspector[Int, DefaultWeightedEdge](inputGraph).connectedSets();
    sets
  }

  def runComponentWiseOptimization() = {
    logger.debug(s"Starting Clique Partitioning Optimization")
    val components = getComponents(inputGraph)
    logger.debug(s"Input Graph has ${inputGraph.vertexSet().size()} vertices and ${inputGraph.edgeSet().size()} edges and ${components.size()} connected components")
    var i = 0
    val componentIterator = components.iterator()
    while(componentIterator.hasNext){
      val c = componentIterator.next()
      val subgraph = new NewSubgraph(new AsSubgraph(inputGraph, c))
      optimizeComponent(subgraph)
      i+=1
      if(i%1000==0) {
        logger.debug(s"Finished $i connected components (${100*i/components.size()}%)")
      }
    }
    closeAllWriters()
  }



}
