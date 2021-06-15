package de.hpi.tfm.evaluation.data

import com.typesafe.scalalogging.StrictLogging
import de.hpi
import de.hpi.tfm
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.evaluation
import de.hpi.tfm.evaluation.data
import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScore
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//adjacency list only contains every edge once (from the lower index to the higher index)! First element of adjacency list is the node index, second is the score
case class SLimGraph(verticesOrdered: IndexedSeq[String], adjacencyList: collection.Map[Int, collection.Map[Int, Float]]) extends JsonWritable[SLimGraph] with StrictLogging{

  assert(adjacencyList.forall(_._2.forall(!_._2.isNegInfinity)))

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

  //serializes this as an adjacencyMatrix file
  def serializeToMDMCPInputFile(f:File) = {
    val pr = new PrintWriter(f)
    pr.println(s" ${verticesOrdered.size}")
    verticesOrdered
      .zipWithIndex
      .foreach{case (_,i) => {
        val neighbors = adjacencyList.getOrElse(i,Map[Int,Float]())
        val weights = Seq(0) ++ ((i+1) until verticesOrdered.size).map{ j =>
          val weight:Float = neighbors.getOrElse(j,Float.MinValue)
          getScoreAsInt(weight)
        }
        pr.println(weights.mkString("  "))
        if(i%1000==0){
          logger.debug(s"Done with $i (${100*i/verticesOrdered.size.toDouble}%)")
        }
      }}
    pr.close()
  }

  def transformToOptimizationGraph = {
    val newVertices = scala.collection.mutable.HashSet[Int]() ++ (0 until verticesOrdered.size)
    val newEdges = adjacencyList.flatMap{case (v1,adjList) => {
      adjList.map{case (v2,weight) => WUnDiEdge(v1,v2)(weight)}
    }}
    val graph = Graph.from(newVertices,newEdges)
    graph
  }

}
object SLimGraph extends JsonReadable[SLimGraph] with StrictLogging {

  def fromGeneralEdgeIterator(edges: hpi.tfm.evaluation.data.GeneralEdge.JsonObjectPerLineFileIterator, scoringFunction: MultipleEventWeightScore[Any], scoringFunctionThreshold: Double) = {
    val vertices = collection.mutable.HashSet[String]()
    val adjacencyList = collection.mutable.HashMap[String, mutable.HashMap[String, Float]]()
    var count = 0
    edges.foreach(e => {
      vertices.add(e.v1.id)
      vertices.add(e.v2.id)
      val score = scoringFunction.compute(e.v1.factLineage.toFactLineage,e.v2.factLineage.toFactLineage)
      var weight = (score-scoringFunctionThreshold).toFloat
      if(weight==Float.NegativeInfinity)
        weight = Float.MinValue
      //val scoreAsInt = getScoreAsInt(e,scoringFunction,scoringFunctionThreshold)
      assert(e.v1.id!=e.v2.id)
      if(e.v1.id<e.v2.id)
        adjacencyList.getOrElseUpdate(e.v1.id,mutable.HashMap[String,Float]()).put(e.v2.id,weight)
      else {
        adjacencyList.getOrElseUpdate(e.v2.id,mutable.HashMap[String,Float]()).put(e.v1.id,weight)
      }
      count+=1
      if(count%100000==0)
        logger.debug(s"Done with $count edges")
    })
    val verticesOrdered = vertices.toIndexedSeq.sorted
    val nameToIndexMap = verticesOrdered
      .zipWithIndex
      .toMap
    val adjacencyListAsInt = adjacencyList.map{case (stringKey,stringKeyMap) =>{
      (nameToIndexMap(stringKey),stringKeyMap.map{case (k,v) => (nameToIndexMap(k),v)})
    }}
    SLimGraph(verticesOrdered,adjacencyListAsInt)
  }

}
