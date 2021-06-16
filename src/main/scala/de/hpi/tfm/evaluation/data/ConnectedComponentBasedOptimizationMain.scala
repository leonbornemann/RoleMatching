package de.hpi.tfm.evaluation.data

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.fact_merging.optimization.{GreedyEdgeBasedOptimizer, SubGraph}
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.io.File

object ConnectedComponentBasedOptimizationMain extends App {
//  case class lol(a:Double) extends JsonWritable[lol]
//  object lol extends JsonReadable[lol]
//
//  val a = lol(Double.NegativeInfinity)
//  private val jsonString: String = a.toJson()
//  println(jsonString)
//  val b = lol.fromJsonString(jsonString)
//  println(b)

  //test this a bit
//  val vertices = Seq(4,7,2,3,100)
//  val edges = Set(WUnDiEdge(2,3)(-0.7),WUnDiEdge(3,4)(0.5),WUnDiEdge(4,7)(-0.5),WUnDiEdge(7,100)(0.2))
//  val graph1 = Graph.from(vertices,edges)
//  val subGraph = new SubGraph(graph1)
//  subGraph.toMDMCPInputFile(new File("testNew.txt"))
//  assert(false)
  val slimGraphFile = args(0)
  val resultFile = new File(args(1))
  val componentDir = args(2)
  val graph = SLimGraph.fromJsonFile(slimGraphFile)
  val optimizer = new GreedyEdgeBasedOptimizer(graph.transformToOptimizationGraph,resultFile)
  val components = optimizer.componentIterator()
  var curComponentID = 0
  components.foreach(c => {
    val name = c.componentName
    if(c.nVertices<8){
      //we can do brute-force easily enough
    } else if(c.nVertices>=8 && c.nVertices<500){
      //use related work MDMCP approach
      c.toMDMCPInputFile(new File(componentDir + s"/$name.txt"))
    } else {
      //component is too large - use greedy
    }
    curComponentID+=1
  })
  //optimizer.printComponentSizeHistogram()
}
