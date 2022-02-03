package de.hpi.role_matching.cbrm.compatibility_graph

import de.hpi.role_matching.cbrm.sgcp.{ComponentWiseOptimizer, Histogram, NewSubgraph}
import org.jgrapht.graph.{DefaultWeightedEdge, SimpleWeightedGraph}

import java.io.File

class ComponentSizerPrinter(graph: SimpleWeightedGraph[Int, DefaultWeightedEdge], resultFile: File) extends ComponentWiseOptimizer(graph,resultFile){

  var histogram = collection.mutable.ArrayBuffer[Int]()

  override def optimizeComponent(subGraph: NewSubgraph): Unit = {
    histogram.append(subGraph.nVertices)
  }

  override def closeAllWriters(): Unit = {
    val hist = Histogram(histogram)
    hist.printAll()
    hist.toJsonFile(resultFile)
  }
}
