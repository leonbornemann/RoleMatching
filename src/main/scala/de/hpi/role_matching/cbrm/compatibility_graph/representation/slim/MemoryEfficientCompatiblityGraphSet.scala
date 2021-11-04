package de.hpi.role_matching.cbrm.compatibility_graph.representation.slim

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.data.Roleset
import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.role_matching.cbrm.evidence_based_weighting.EventCounts
import de.hpi.role_matching.cbrm.sgcp.ScoreConfig
import org.jgrapht.Graph
import org.jgrapht.graph.{DefaultWeightedEdge, SimpleWeightedGraph}

import java.time.LocalDate

//if Seq[EventCountsWithoutWeights].size in an edge is smaller than trainTimeEnds.size the later points in time do not exist
case class MemoryEfficientCompatiblityGraphSet(verticesOrdered: IndexedSeq[String],
                                               trainTimeEnds: Seq[LocalDate],
                                               adjacencyList: collection.Map[Int, collection.Map[Int, Seq[EventCounts]]]) extends JsonWritable[MemoryEfficientCompatiblityGraph] with StrictLogging {

  def getBaselineNoWeightSetting(trainTimeEnd: LocalDate) = {
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


  def isValidEdge(v1: Int, v2: Int, vertexLookupMap: Roleset): Boolean = {
    vertexLookupMap.posToRoleLineage(v1).tryMergeWithConsistent(vertexLookupMap.posToRoleLineage(v2)).isDefined
  }

  def getMaxRecallSettingOptimizationGraph(trainTimeEnd: LocalDate, vertexLookupMap:Roleset) = {
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
object MemoryEfficientCompatiblityGraphSet extends JsonReadable[MemoryEfficientCompatiblityGraphSet]
