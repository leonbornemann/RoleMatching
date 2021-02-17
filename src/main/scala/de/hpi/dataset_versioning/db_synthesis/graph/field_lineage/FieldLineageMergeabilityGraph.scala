package de.hpi.dataset_versioning.db_synthesis.graph.field_lineage

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{IDBasedTupleReference, TupleReference}
import de.hpi.dataset_versioning.db_synthesis.graph.association.{AssociationMergeabilityGraph, AssociationMergeabilityGraphEdge}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService.{FIELD_LINEAGE_MERGEABILITY_GRAPH_DIR, createParentDirs}
import scalax.collection.Graph
import scalax.collection.edge.WLkUnDiEdge

import java.io.File

case class FieldLineageMergeabilityGraph(edges: IndexedSeq[FieldLineageGraphEdge]) extends JsonWritable[FieldLineageMergeabilityGraph]{

  def transformToOptimizationGraph[A](inputTables: Map[DecomposedTemporalTableIdentifier, TemporalDatabaseTableTrait[A]]) = {
    val newVertices = scala.collection.mutable.HashSet[TupleReference[A]]()
    val newEdges = edges.map(e => {
      val tr1 = getTupleReference[A](e.tupleReferenceA,inputTables(e.tupleReferenceA.associationID))
      val tr2 = getTupleReference[A](e.tupleReferenceB,inputTables(e.tupleReferenceB.associationID))
      val edgeScore = GLOBAL_CONFIG.OPTIMIZATION_TARGET_FUNCTION(tr1,tr2)
      newVertices.add(tr1)
      newVertices.add(tr2)
      WLkUnDiEdge(tr1,tr2)(edgeScore,edgeScore)
    })
    val graph = Graph.from(newVertices,newEdges)
    graph
  }

  private def getTupleReference[A](e: IDBasedTupleReference,temporalDatabaseTableTrait: TemporalDatabaseTableTrait[A]) = {
    e.toTupleReference(temporalDatabaseTableTrait)
  }

  //discard evidence to save memory
  def withoutEvidenceSets = FieldLineageMergeabilityGraph(edges
    .map(e => FieldLineageGraphEdge(e.tupleReferenceA,e.tupleReferenceB,e.evidence,None)))

  def transformToTableGraph = {
    val tableGraphEdges = edges
      .groupMap(e => Set(e.tupleReferenceA.associationID,e.tupleReferenceB.associationID))(e => (e.evidence,e.evidenceSet.get))
      .toIndexedSeq
      .withFilter{case (k,v) =>{
        if(!v.forall(t => t._1 == t._2.map(_._2).sum)){
          println(k)
          val failed = v.filter(t => t._1 != t._2.map(_._2).sum)
          failed.foreach(println(_))
        }
        assert(v.forall(t => t._1 == t._2.map(_._2).sum))
        v.map(_._1).sum>0
      }}
      .map{case (k,v) => {
        assert(k.size==2)
        val keyList = k.toIndexedSeq
        val evidenceMultiSet = v.flatMap(_._2.toIndexedSeq)
          .groupMap(_._1)(_._2)
          .map{case (k,v) => (k,v.sum)}
          .toIndexedSeq
        val summedEvidence = v.map(_._1).sum
        AssociationMergeabilityGraphEdge(keyList(0),keyList(1),summedEvidence,evidenceMultiSet)
      }}
    AssociationMergeabilityGraph(tableGraphEdges)
  }


  def idSet:Set[DecomposedTemporalTableIdentifier] = edges.flatMap(e => Set[DecomposedTemporalTableIdentifier](e.tupleReferenceA.associationID,e.tupleReferenceB.associationID)).toSet

  def writeToStandardFile() = {
    toJsonFile(FieldLineageMergeabilityGraph.getFieldLineageMergeabilityGraphFile(idSet))
  }

}
object FieldLineageMergeabilityGraph extends JsonReadable[FieldLineageMergeabilityGraph] with StrictLogging{

  def idsFromFilename(f: File) = {
    val tokens = f.getName.split(";")
    val tokensFinal = tokens.slice(0,tokens.size-1) ++ Seq(tokens.last.replace(".json",""))
    tokensFinal.map(DecomposedTemporalTableIdentifier.fromCompositeID(_)).toSet
  }

  def loadSubGraph(inputTableIDs: Set[DecomposedTemporalTableIdentifier], subdomain:String, discardEvidenceSet:Boolean = true) = {
    val files = getFieldLineageMergeabilityFiles(subdomain)
    val subGraphEdges = files
      .filter(f => {
        val idsInFile = idsFromFilename(f)
        idsInFile.forall(inputTableIDs.contains(_))
      })
      .flatMap(f => fromJsonFile(f.getAbsolutePath).withoutEvidenceSets.edges)
    FieldLineageMergeabilityGraph(subGraphEdges)
  }

  def readFieldLineageMergeabilityGraphAndAggregateToTableGraph(subdomain:String, fileCountLimit:Int = Integer.MAX_VALUE) = {
    var count = 0
    val allEdges = getFieldLineageMergeabilityFiles(subdomain)
      .take(fileCountLimit)
      .toIndexedSeq
      .flatMap(f => {
        val tg = fromJsonFile(f.getAbsolutePath).transformToTableGraph
        count +=1
        if(count%100==0)
          logger.debug(s"Read $count files")
        tg.edges
      })
    AssociationMergeabilityGraph(allEdges)
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
