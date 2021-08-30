package de.hpi.role_matching.compatibility.graph.representation.slim

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.clique_partitioning.ScoreConfig
import de.hpi.socrata.{JsonReadable, JsonWritable}
import de.hpi.role_matching.scoring.EventCountsWithoutWeights
import org.jgrapht.Graph
import org.jgrapht.graph.{DefaultWeightedEdge, SimpleGraph, SimpleWeightedGraph}

import java.time.LocalDate

//if Seq[EventCountsWithoutWeights].size in an edge is smaller than trainTimeEnds.size the later points in time do not exist
case class SlimGraphSet(verticesOrdered: IndexedSeq[String],
                        trainTimeEnds: Seq[LocalDate],
                        adjacencyList: collection.Map[Int, collection.Map[Int, Seq[EventCountsWithoutWeights]]]) extends JsonWritable[SLimGraph] with StrictLogging {
  def getBaselineNoWeightSetting(trainTimeEnd: LocalDate, vertexLookupMap: VertexLookupMap) = {
    val newVertices = scala.collection.mutable.HashSet[Int]() ++ (0 until verticesOrdered.size)
    assert(trainTimeEnds.contains(trainTimeEnd))
    val indexOfTrainTimeEnd = trainTimeEnds.indexOf(trainTimeEnd)
    val graph:Graph[Int,DefaultWeightedEdge] = new SimpleWeightedGraph[Int,DefaultWeightedEdge](classOf[DefaultWeightedEdge])
    newVertices.foreach(v => graph.addVertex(v))
    adjacencyList.foreach{case (v1,adjListThisNode) => {
      adjListThisNode
        .withFilter{case (_,eventCounts) => eventCounts.size>indexOfTrainTimeEnd}
        .foreach{case (v2,_) => {
          val score = 1.0f
          val e = graph.addEdge(v1,v2)
          graph.setEdgeWeight(e,score)
        }}
    }}
    graph
  }


  def isValidEdge(v1: Int, v2: Int, vertexLookupMap: VertexLookupMap): Boolean = {
    vertexLookupMap.posToFactLineage(v1).tryMergeWithConsistent(vertexLookupMap.posToFactLineage(v2)).isDefined
  }

  def getMaxRecallSettingOptimizationGraph(trainTimeEnd: LocalDate, vertexLookupMap:VertexLookupMap) = {
    val newVertices = scala.collection.mutable.HashSet[Int]() ++ (0 until verticesOrdered.size)
    assert(trainTimeEnds.contains(trainTimeEnd))
    val indexOfTrainTimeEnd = trainTimeEnds.indexOf(trainTimeEnd)
    val graph:Graph[Int,DefaultWeightedEdge] = new SimpleWeightedGraph[Int,DefaultWeightedEdge](classOf[DefaultWeightedEdge])
    newVertices.foreach(v => graph.addVertex(v))
    adjacencyList.foreach{case (v1,adjListThisNode) => {
      adjListThisNode
        .withFilter{case (_,eventCounts) => eventCounts.size>indexOfTrainTimeEnd}
        .foreach{case (v2,_) => {
          val score = if(isValidEdge(v1,v2,vertexLookupMap)) 1.0f else 0.0f
          if(score == Float.NegativeInfinity){
            println()
          }
          val e = graph.addEdge(v1,v2)
          graph.setEdgeWeight(e,score)
        }}
    }}
    graph
  }

  def transformToOptimizationGraph(trainTimeEnd: LocalDate, weightConfig: ScoreConfig) = {
    val newVertices = scala.collection.mutable.HashSet[Int]() ++ (0 until verticesOrdered.size)
    assert(trainTimeEnds.contains(trainTimeEnd))
    val indexOfTrainTimeEnd = trainTimeEnds.indexOf(trainTimeEnd)
    val graph = new SimpleWeightedGraph[Int,DefaultWeightedEdge](classOf[DefaultWeightedEdge])
    newVertices.foreach(v => graph.addVertex(v))
    adjacencyList.foreach{case (v1,adjListThisNode) => {
      adjListThisNode
        .withFilter{case (_,eventCounts) => eventCounts.size>indexOfTrainTimeEnd}
        .map{case (v2,eventCounts) => {
          val score = weightConfig.computeScore(eventCounts(indexOfTrainTimeEnd))
          val e = graph.addEdge(v1,v2)
          graph.setEdgeWeight(e,score)
        }}
    }}
    graph
  }

}
object SlimGraphSet extends JsonReadable[SlimGraphSet]
