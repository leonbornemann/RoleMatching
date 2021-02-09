package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService
import scalax.collection.GraphEdge.UnDiEdge
import scalax.collection.edge.WUnDiEdge
import scalax.collection.immutable.Graph

case class AssociationMergeabilityGraph(edges: IndexedSeq[AssociationMergeabilityGraphEdge]) extends JsonWritable[AssociationMergeabilityGraph]{

  def toScalaGraph = {
    val asWundiedges = edges.map(e => WUnDiEdge(e.v1, e.v2)(e.summedEvidence))
    val nodes = edges.toSet.flatMap(e => Set(e.v1,e.v2))
    val graph = Graph.from(nodes,asWundiedges)
    graph

  }


  def writeToStandardFile(subdomain:String) = {
    toJsonFile(DBSynthesis_IOService.getAssociationMergeabilityGraphFile(subdomain))
  }

}
object AssociationMergeabilityGraph extends JsonReadable[AssociationMergeabilityGraph]{

  def readFromStandardFile(subdomain:String) = {
    fromJsonFile(DBSynthesis_IOService.getAssociationMergeabilityGraphFile(subdomain).getAbsolutePath)
  }

}
