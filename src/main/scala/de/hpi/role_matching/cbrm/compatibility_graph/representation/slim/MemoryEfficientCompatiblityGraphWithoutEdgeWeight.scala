package de.hpi.role_matching.cbrm.compatibility_graph.representation.slim

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.{SimpleCompatbilityGraphEdge, SimpleCompatibilityGraphEdgeIterator}
import de.hpi.role_matching.cbrm.data.RoleLineageWithID
import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.role_matching.cbrm.evidence_based_weighting.isf.ISFMapStorage

import java.time.LocalDate
import scala.collection.mutable

//the boolean arrays show whether a vertex/edge is present in the graph at traintTimeEnds[i]
case class MemoryEfficientCompatiblityGraphWithoutEdgeWeight(smallestTrainTimeEnd:LocalDate,
                                                             trainTimeEnds:Seq[LocalDate],
                                                             verticesOrdered: IndexedSeq[RoleLineageWithID],
                                                             adjacencyList: collection.Map[Int, collection.Map[Int,Seq[Boolean]]]) extends JsonWritable[MemoryEfficientCompatiblityGraphWithoutEdgeWeight] with StrictLogging{
  def getISFMapsAtEndTimes(trainTimeEnds: Array[LocalDate]) = {
    trainTimeEnds.map(t => {
      val lineagesProjected = verticesOrdered.map(rl => RoleLineageWithID(rl.id,rl.roleLineage
          .toRoleLineage
          .projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,t).toSerializationHelper)
      )
      val hist = RoleLineageWithID.getTransitionHistogramForTFIDFFromVertices(lineagesProjected, GLOBAL_CONFIG.granularityInDays)
      (t,ISFMapStorage(hist.toIndexedSeq))
    })
      .toMap
  }


  def getLineage(vertex: Int): RoleLineageWithID = {
    verticesOrdered(vertex)
  }

  def generalEdgeIterator = {
    new SimpleCompatibilityGraphEdgeIterator(this)
  }

  def allEndTimes = Set(smallestTrainTimeEnd) ++ trainTimeEnds
}
object MemoryEfficientCompatiblityGraphWithoutEdgeWeight extends JsonReadable[MemoryEfficientCompatiblityGraphWithoutEdgeWeight] with StrictLogging {

  def fromGeneralEdgeIterator(edges: SimpleCompatbilityGraphEdge.JsonObjectPerLineFileIterator,
                              trainTimeStart:LocalDate,
                              smallestTrainTimeEnd:LocalDate,
                              trainTimeEnds:Seq[LocalDate]) = {
    val vertices = collection.mutable.HashSet[RoleLineageWithID]()
    val adjacencyList = collection.mutable.HashMap[String, mutable.HashMap[String, Seq[Boolean]]]()
    var count = 0
    var skipped = 0
    edges.foreach(e => {
      vertices.add(e.v1)
      vertices.add(e.v2)
      val fl1 = e.v1.roleLineage.toRoleLineage
      val fl2 = e.v2.roleLineage.toRoleLineage
      if(fl1.projectToTimeRange(trainTimeStart,smallestTrainTimeEnd).tryMergeWithConsistent(fl2.projectToTimeRange(trainTimeStart,smallestTrainTimeEnd)).isDefined){
        val edgeIsPresent = trainTimeEnds.map(end => fl1.projectToTimeRange(trainTimeStart,end).tryMergeWithConsistent(fl2.projectToTimeRange(trainTimeStart,end)).isDefined)
        val id1 = e.v1.id
        val id2 = e.v2.id
        if(id1 < id2){
          adjacencyList.getOrElseUpdate(id1,mutable.HashMap[String, Seq[Boolean]]()).put(id2,edgeIsPresent)
        } else {
          adjacencyList.getOrElseUpdate(id2,mutable.HashMap[String, Seq[Boolean]]()).put(id1,edgeIsPresent)
        }
      } else {
        skipped+=1
      }
      count+=1
      if(count%100000==0)
        logger.debug(s"Done with $count edges")
    })
    logger.debug(s"Skipped $skipped edges - weird")
    fromVerticesAndEdges(vertices,adjacencyList,smallestTrainTimeEnd,trainTimeEnds)
  }

  def fromVerticesAndEdges(vertices: collection.Set[RoleLineageWithID],
                           adjacencyList: collection.Map[String, collection.Map[String, Seq[Boolean]]],
                           smallestTrainTimeEnd:LocalDate,
                           trainTimeEnds:Seq[LocalDate]) = {
    val verticesOrdered = vertices.toIndexedSeq.sortBy(_.id)
    val nameToIndexMap = verticesOrdered
      .map(_.id)
      .zipWithIndex
      .toMap
    val adjacencyListAsInt = adjacencyList.map{case (stringKey,stringKeyMap) =>{
      (nameToIndexMap(stringKey),stringKeyMap.map{case (k,v) => (nameToIndexMap(k),v)})
    }}
    MemoryEfficientCompatiblityGraphWithoutEdgeWeight(smallestTrainTimeEnd,trainTimeEnds,verticesOrdered,adjacencyListAsInt)
  }
}
