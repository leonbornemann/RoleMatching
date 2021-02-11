package de.hpi.dataset_versioning.db_synthesis.preparation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.General_1_to_1_TupleMatching
import de.hpi.dataset_versioning.db_synthesis.preparation.FieldLineageMergeabilityGraph.getFieldLineageMergeabilityGraphFile
import de.hpi.dataset_versioning.io.DBSynthesis_IOService
import de.hpi.dataset_versioning.io.DBSynthesis_IOService.{FIELD_LINEAGE_MERGEABILITY_GRAPH_DIR, createParentDirs}

import java.io.File

case class FieldLineageMergeabilityGraph(edges: IndexedSeq[FieldLineageGraphEdge]) extends JsonWritable[FieldLineageMergeabilityGraph]{

  def transformToTableGraph = {
    val tableGraph = edges.groupMap(e => Set(e.tupleReferenceA.associationID,e.tupleReferenceB.associationID))(e => e.evidence)
      .withFilter(_._2.sum>0)
      .map{case (k,v) => (k,v.sum)}
    tableGraph
  }


  def idSet:Set[DecomposedTemporalTableIdentifier] = edges.flatMap(e => Set[DecomposedTemporalTableIdentifier](e.tupleReferenceA.associationID,e.tupleReferenceB.associationID)).toSet

  def writeToStandardFile() = {
    toJsonFile(getFieldLineageMergeabilityGraphFile(idSet))
  }

}
object FieldLineageMergeabilityGraph extends JsonReadable[FieldLineageMergeabilityGraph] with StrictLogging{

  def readTableGraph(subdomain:String) = {
    var count = 0
    val allEdges = getFieldLineageMergeabilityFiles(subdomain)
      .toIndexedSeq
      .flatMap(f => {
        val tg = fromJsonFile(f.getAbsolutePath).transformToTableGraph
        assert(tg.size<=1)
        count +=1
        if(count%100==0)
          logger.debug(s"Read $count files")
        tg
      })
    allEdges
  }

  def readAllBipartiteGraphs(subdomain:String) = {
    val allEdges = getFieldLineageMergeabilityFiles(subdomain)
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
    fromJsonFile(getFieldLineageMergeabilityGraphFile(ids).getAbsolutePath)
  }

  def getFieldLineageMergeabilityFiles(subdomain:String) = {
    new File(FIELD_LINEAGE_MERGEABILITY_GRAPH_DIR + s"/${subdomain}/").listFiles()
  }

  def getFieldLineageMergeabilityGraphFile(ids: Set[DecomposedTemporalTableIdentifier]): File = {
    val idString = ids.toIndexedSeq.map(_.compositeID).sorted.mkString(";")
    val subdomain = ids.map(_.subdomain).toSet
    if(subdomain.size!=1)
      println()
    assert(subdomain.size==1)
    createParentDirs(new File(FIELD_LINEAGE_MERGEABILITY_GRAPH_DIR + s"/${subdomain.head}/" + idString + ".json"))
  }

}
