package de.hpi.role_matching.compatibility.graph.representation.slim

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.clique_partitioning.ScoreConfig
import de.hpi.socrata.{JsonReadable, JsonWritable}
import de.hpi.role_matching.scoring.EventCountsWithoutWeights
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.time.LocalDate

//if Seq[EventCountsWithoutWeights].size in an edge is smaller than trainTimeEnds.size the later points in time do not exist
case class SlimGraphSet(verticesOrdered: IndexedSeq[String],
                        trainTimeEnds: Seq[LocalDate],
                        adjacencyList: collection.Map[Int, collection.Map[Int, Seq[EventCountsWithoutWeights]]]) extends JsonWritable[SLimGraph] with StrictLogging {

  def transformToOptimizationGraph(trainTimeEnd: LocalDate, weightConfig: ScoreConfig) = {
    val newVertices = scala.collection.mutable.HashSet[Int]() ++ (0 until verticesOrdered.size)
    val indexOfTrainTimeEnd = trainTimeEnds.indexOf(trainTimeEnd)
    val newEdges = adjacencyList.flatMap{case (v1,adjListThisNode) => {
      adjListThisNode
        .withFilter{case (_,eventCounts) => eventCounts.size>indexOfTrainTimeEnd}
        .map{case (v2,eventCounts) => {
          val score = weightConfig.computeScore(eventCounts(indexOfTrainTimeEnd))
          WUnDiEdge(v1,v2)(score)
        }}
    }}
    val graph = Graph.from(newVertices,newEdges)
    graph
  }

}
object SlimGraphSet extends JsonReadable[SlimGraphSet]
