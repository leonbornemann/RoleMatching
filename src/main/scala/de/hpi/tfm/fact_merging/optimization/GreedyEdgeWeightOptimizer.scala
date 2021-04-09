package de.hpi.tfm.fact_merging.optimization

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import scalax.collection.Graph
import scalax.collection.edge.WLkUnDiEdge

import java.io.{File, PrintWriter}

class GreedyEdgeWeightOptimizer(subdomain: String, connectedComponentListFile: File,graphConfig: GraphConfig) extends ConnectedComponentMergeOptimizer(subdomain,connectedComponentListFile,graphConfig) with StrictLogging{

  var chosenmerges = scala.collection.mutable.HashSet[TupleMerge]()

  def run() = {
    logger.debug(s"Starting Clique Partitioning Optimization for $connectedComponentListFile")
    logger.debug(s"Input Graph has ${inputGraph.nodes.size} vertices and ${inputGraph.edges.size} edges and ${inputGraph.componentTraverser().size} connected components")
    val traverser = inputGraph.componentTraverser()
    val pr = new PrintWriter(TupleMerge.getStandardJsonObjectPerLineFile(subdomain,
      GreedyEdgeWeightOptimizer.methodName,
      GLOBAL_CONFIG.OPTIMIZATION_TARGET_FUNCTION_NAME,
      connectedComponentListFile.getName))
    var numNonTrivialComponents = 0
    traverser.foreach(e => {
      val subGraph: Graph[TupleReference[Any], WLkUnDiEdge] = componentToGraph(e)
      logger.debug(s"Handling Component with Vertices: ${subGraph.nodes.map(_.value)}")
      logger.debug(s"Vertex Count: ${subGraph.nodes.size}, edge count: ${subGraph.edges.size}")
      val optimizer = new GreedyEdgeWeightOptimizerForComponent(subGraph)
      val componentMerges = optimizer.mergeGreedily()
      assert(componentMerges.toIndexedSeq.flatMap(_.clique).size==subGraph.nodes.size)
      componentMerges.foreach(tm => {
        tm.appendToWriter(pr,false,true)
      })
      if(subGraph.edges.size>subGraph.nodes.size-1){
        numNonTrivialComponents +=1
      }
    })
    pr.close()
    logger.debug(s"Found $numNonTrivialComponents nonTrivialComponents")
  }
}

object GreedyEdgeWeightOptimizer{
  val methodName = "GreedyEdgeWeightOptimizerMain"
}

