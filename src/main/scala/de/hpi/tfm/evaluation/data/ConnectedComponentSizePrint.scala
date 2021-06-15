package de.hpi.tfm.evaluation.data

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.fact_merging.optimization.GreedyEdgeBasedOptimizer

import java.io.File

object ConnectedComponentSizePrint extends App {
  case class lol(a:Double) extends JsonWritable[lol]
  object lol extends JsonReadable[lol]

  val a = lol(Double.NegativeInfinity)
  private val jsonString: String = a.toJson()
  println(jsonString)
  val b = lol.fromJsonString(jsonString)
  println(b)

  val slimGraphFile = args(0)
  val resultFile = new File(args(1))
  val graph = SLimGraph.fromJsonFile(slimGraphFile)
  val optimizer = new GreedyEdgeBasedOptimizer(graph.transformToOptimizationGraph,resultFile)
  optimizer.printComponentSizeHistogram()
}
