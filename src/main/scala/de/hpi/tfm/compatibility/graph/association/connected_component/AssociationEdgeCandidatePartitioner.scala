package de.hpi.tfm.compatibility.graph.association.connected_component

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.graph.association.AssociationGraphEdgeCandidate
import de.hpi.tfm.io.{DBSynthesis_IOService, IOService}

import java.io.PrintWriter

object AssociationEdgeCandidatePartitioner extends App with StrictLogging{
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val maxPartitionSize = 20
  val candidateFile = DBSynthesis_IOService.getAssociationGraphEdgeCandidateFile(subdomain)
  val edges = AssociationGraphEdgeCandidate.fromJsonObjectPerLineFile(candidateFile.getAbsolutePath)
  assert(edges.toSet.size==edges.size)
  val byFirst = edges.groupBy(_.firstMatchPartner)
    .map(t => (t._1,t._2.toSet))
  val bySecond = edges.groupBy(_.secondMatchPartner)
    .map(t => (t._1,t._2.toSet))
  val adjacencyList = byFirst ++ bySecond
  //assert all edges are retained
  assert(adjacencyList.values.toSet.flatten.size==edges.size)
  var edgeLists = scala.collection.mutable.ArrayBuffer[IndexedSeq[AssociationGraphEdgeCandidate]]()
  val representedEdges = scala.collection.mutable.HashSet[AssociationGraphEdgeCandidate]()
  val dir = DBSynthesis_IOService.getAssociationGraphEdgeCandidatePartitionDir(subdomain)
  var curPartitionNum = 0
  var totalSerializedEdges = 0
  val curPartition = collection.mutable.ArrayBuffer[AssociationGraphEdgeCandidate]()
  adjacencyList.foreach{case (key,edges) => {
    val unseenEdges = edges.diff(representedEdges)
      .toIndexedSeq
    assert(unseenEdges.forall(e => !representedEdges.contains(e)))
    unseenEdges.foreach(e => {
      curPartition += e
      if(curPartition.size>=maxPartitionSize){
        clearAndSerializePartition
      }
    })
  }}
  if(curPartition.size>1)
    clearAndSerializePartition
  if(totalSerializedEdges != edges.toSet.size){
    println("What?")
    representedEdges.diff(edges.toSet)
      .foreach(println)
    println(totalSerializedEdges == representedEdges.size)
    println(totalSerializedEdges)
    println(edges.toSet.size)
    //assert(false)
  }
  if(representedEdges.toSet != edges.toSet){
    println("oh oh")
    println(representedEdges.toSet.size)
    println(edges.toSet.size)
  }
  assert(representedEdges.toSet == edges.toSet)


  private def clearAndSerializePartition = {
    val file = DBSynthesis_IOService.getAssociationGraphEdgeCandidatePartitionFile(subdomain, curPartitionNum)
    val pr = new PrintWriter(file)
    curPartition.foreach(e => {
      totalSerializedEdges += 1
      e.appendToWriter(pr, false, true)
      representedEdges += e
    })
    pr.close()
    logger.debug(s"Finished partition $curPartitionNum")
    curPartitionNum += 1
    curPartition.clear()
  }

}
