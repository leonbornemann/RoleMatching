package de.hpi.tfm.compatibility.graph.association.connected_component

import de.hpi.tfm.compatibility.graph.association.AssociationGraphEdgeCandidate
import de.hpi.tfm.io.{DBSynthesis_IOService, IOService}

import java.io.PrintWriter

object AssociationEdgeCandidatePartitioner extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val maxPartitionSize = 10
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
  adjacencyList.foreach{case (key,edges) => {
    val unseenEdges = edges.diff(representedEdges)
      .toIndexedSeq
    assert(unseenEdges.forall(e => !representedEdges.contains(e)))
    val partitions = (0 until (unseenEdges.size / maxPartitionSize.toDouble).ceil.toInt)
      .map(i => unseenEdges.slice(i*maxPartitionSize,(i+1)*maxPartitionSize))
    partitions.foreach(p => {
      val file = DBSynthesis_IOService.getAssociationGraphEdgeCandidatePartitionFile(subdomain,curPartitionNum)
      val pr = new PrintWriter(file)
      p.foreach(e => {
        totalSerializedEdges +=1
        e.appendToWriter(pr,false,true)
        representedEdges += e
      })
      pr.close()
      curPartitionNum +=1
    })
  }}
  if(totalSerializedEdges != edges.size){
    println(totalSerializedEdges)
    println(edges.size)
    assert(false)
  }



}
