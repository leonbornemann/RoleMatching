package de.hpi.tfm.fact_merging.optimization

import com.typesafe.scalalogging.StrictLogging
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.io.{File, PrintWriter}

class SubGraph(val graph: Graph[Int, WUnDiEdge]) extends StrictLogging{

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
        val weights = Seq(0) ++ ((i+1) until verticesOrdered.size).map{ w =>
          val weight:Float = neighborsSorted.getOrElse(w,Float.MinValue).toFloat
          getScoreAsInt(weight)
        }
        pr.println(weights.mkString("  "))
        if(i%1000==0){
          logger.debug(s"Done with $i (${100*i/verticesOrdered.size.toDouble}%)")
        }
      }}
    pr.close()
  }

  val scoreRangeIntMin = Integer.MIN_VALUE / 1000.0
  val scoreRangeIntMax = Integer.MAX_VALUE / 1000.0
  val scoreRangeDoubleMin = -1.0.toFloat
  val scoreRangeDoubleMax = 1.0.toFloat

  //Tranfer x from scale [a,b] to y in scale [c,d]
  // (x-a) / (b-a) = (y-c) / (d-c)
  //
  //y = (d-c)*(x-a) / (b-a) +c
  def scaleInterpolation(x: Double, a: Double, b: Double, c: Double, d: Double) = {
    val y = (d-c)*(x-a) / (b-a) +c
    assert(y >=c && y <= d)
    y
  }

  def getScoreAsInt(weight:Float):Int = {
    assert( weight==Float.MinValue || weight >= scoreRangeDoubleMin && weight <= scoreRangeDoubleMax)
    val scoreAsInt = if(weight==Float.MinValue) Integer.MIN_VALUE else scaleInterpolation(weight,scoreRangeDoubleMin,scoreRangeDoubleMax,scoreRangeIntMin,scoreRangeIntMax).round.toInt
    scoreAsInt
  }

  def nVertices = graph.nodes.size
}
