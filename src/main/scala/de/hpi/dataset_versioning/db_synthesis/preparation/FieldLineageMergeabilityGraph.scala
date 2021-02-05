package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.General_1_to_1_TupleMatching
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

case class FieldLineageMergeabilityGraph(edges: IndexedSeq[FieldLineageGraphEdge]) extends JsonWritable[FieldLineageMergeabilityGraph]{

  def transformToTableGraph = {
    val tableGraph = edges.groupMap(e => Set(e.tupleReferenceA.associationID,e.tupleReferenceB.associationID))(e => e.evidence)
      .withFilter(_._2.sum>0)
      .map{case (k,v) => (k,v.sum)}
    tableGraph
  }


  def idSet:Set[DecomposedTemporalTableIdentifier] = edges.flatMap(e => Set[DecomposedTemporalTableIdentifier](e.tupleReferenceA.associationID,e.tupleReferenceB.associationID)).toSet

  def writeToStandardFile() = {
    toJsonFile(DBSynthesis_IOService.getFieldLineageMergeabilityGraphFile(idSet))
  }

}
object FieldLineageMergeabilityGraph extends JsonReadable[FieldLineageMergeabilityGraph]{

  def readAllBipartiteGraphs(subdomain:String) = {
    val allEdges = DBSynthesis_IOService.getBipartiteMergeabilityGraphFiles(subdomain)
      .toIndexedSeq
      .flatMap(f => fromJsonFile(f.getAbsolutePath).edges)
    //consistency check:
    allEdges.groupBy(t => (t.tupleReferenceA,t.tupleReferenceB))
      .foreach(g => {
        if(g._2.size!=1){
          println(g)
        }
        assert(g._2.size==1)
      })
    FieldLineageMergeabilityGraph(allEdges)
  }

  def readFromStandardFile(ids:Set[DecomposedTemporalTableIdentifier]) = {
    fromJsonFile(DBSynthesis_IOService.getFieldLineageMergeabilityGraphFile(ids).getAbsolutePath)
  }

}
