package de.hpi.tfm.evaluation.data

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.fact_merging.optimization.GreedyEdgeBasedOptimizer

import java.io.File

object ConnectedComponentSizePrint extends App {
//  case class lol(a:Double) extends JsonWritable[lol]
//  object lol extends JsonReadable[lol]
//
//  val a = lol(Double.NegativeInfinity)
//  private val jsonString: String = a.toJson()
//  println(jsonString)
//  val b = lol.fromJsonString(jsonString)
//  println(b)

  val slimGraphFile = args(0)
  val resultFile = new File(args(1))
  val componentDir = args(2)
  val graph = SLimGraph.fromJsonFile(slimGraphFile)
  val optimizer = new GreedyEdgeBasedOptimizer(graph.transformToOptimizationGraph,resultFile)
  val components = optimizer.componentIterator()
  var curComponentID = 0
  components.foreach(c => {
    if(c.nVertices<8){
      //we can do brute-force easily enough
    } else if(c.nVertices>=8 && c.nVertices<500){
      //use related work MDMCP approach
      c.toMDMCPInputFile(new File(componentDir + s"/$curComponentID.txt"))
    } else {
      //component is too large -use greedy
    }
    curComponentID+=1
  })
  //optimizer.printComponentSizeHistogram()
}
