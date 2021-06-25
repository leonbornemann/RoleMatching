package de.hpi.tfm.evaluation.data

import com.typesafe.scalalogging.StrictLogging
import de.hpi
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.evaluation.data.SLimGraph.{fromVerticesAndEdges, getEdgeOption, logger}
import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScore

import java.time.LocalDate
import scala.collection.mutable

//the boolean arrays show whether a vertex/edge is present in the graph at traintTimeEnds[i]
case class SlimGraphWithoutWeight(smallestTrainTimeEnd:LocalDate,
                                  trainTimeEnds:Seq[LocalDate],
                                  verticesOrdered: IndexedSeq[IdentifiedFactLineage],
                                  adjacencyList: collection.Map[Int, collection.Map[Int,Seq[Boolean]]]) extends JsonWritable[SlimGraphWithoutWeight] with StrictLogging{

}
object SlimGraphWithoutWeight extends JsonReadable[SlimGraphWithoutWeight] with StrictLogging {

  def fromGeneralEdgeIterator(edges: hpi.tfm.evaluation.data.GeneralEdge.JsonObjectPerLineFileIterator,
                              trainTimeStart:LocalDate,
                              smallestTrainTimeEnd:LocalDate,
                              trainTimeEnds:Seq[LocalDate]) = {
    val vertices = collection.mutable.HashSet[IdentifiedFactLineage]()
    val adjacencyList = collection.mutable.HashMap[String, mutable.HashMap[String, Seq[Boolean]]]()
    var count = 0
    var skipped = 0
    edges.foreach(e => {
      vertices.add(e.v1)
      vertices.add(e.v2)
      val fl1 = e.v1.factLineage.toFactLineage
      val fl2 = e.v2.factLineage.toFactLineage
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

  def fromVerticesAndEdges(vertices: collection.Set[IdentifiedFactLineage],
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
    SlimGraphWithoutWeight(smallestTrainTimeEnd,trainTimeEnds,verticesOrdered,adjacencyListAsInt)
  }
}
