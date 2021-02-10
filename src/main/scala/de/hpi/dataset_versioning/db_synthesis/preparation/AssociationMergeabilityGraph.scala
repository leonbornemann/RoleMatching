package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService
import scalax.collection.GraphEdge.UnDiEdge
import scalax.collection.edge.{WLkUnDiEdge, WUnDiEdge}
import scalax.collection.immutable.Graph

case class AssociationMergeabilityGraph(edges: IndexedSeq[AssociationMergeabilityGraphEdge]) extends JsonWritable[AssociationMergeabilityGraph]{

  def toScalaGraph = {
    val asWundiedges = edges.map(e => WLkUnDiEdge(e.v1, e.v2)(e.summedEvidence,e.summedEvidence))
    val nodes = edges.flatMap(e => Set(e.v1,e.v2)).toSet
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
