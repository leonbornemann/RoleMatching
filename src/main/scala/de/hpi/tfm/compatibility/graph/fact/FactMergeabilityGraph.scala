package de.hpi.tfm.compatibility.graph.fact

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.association.{AssociationGraphEdge, AssociationMergeabilityGraph}
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.TemporalDatabaseTableTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.DBSynthesis_IOService.{CONNECTED_COMPONENT_DIR, FIELD_LINEAGE_MERGEABILITY_GRAPH_DIR, createParentDirs}
import scalax.collection.Graph
import scalax.collection.edge.WLkUnDiEdge

import java.io.File
import scala.io.Source

case class FactMergeabilityGraph(edges: IndexedSeq[FactMergeabilityGraphEdge],graphConfig: GraphConfig) extends JsonWritable[FactMergeabilityGraph]{

  def transformToOptimizationGraph[A](inputTables: Map[AssociationIdentifier, TemporalDatabaseTableTrait[A]]) = {
    val newVertices = scala.collection.mutable.HashSet[TupleReference[A]]()
    val newEdges = edges.map(e => {
      val tr1 = getTupleReference[A](e.tupleReferenceA,inputTables(e.tupleReferenceA.associationID))
      val tr2 = getTupleReference[A](e.tupleReferenceB,inputTables(e.tupleReferenceB.associationID))
      val edgeScore = GLOBAL_CONFIG.OPTIMIZATION_TARGET_FUNCTION(tr1,tr2)
      newVertices.add(tr1)
      newVertices.add(tr2)
      WLkUnDiEdge(tr1,tr2)(edgeScore,e)
    })
    val graph = Graph.from(newVertices,newEdges)
    graph
  }

  private def getTupleReference[A](e: IDBasedTupleReference,temporalDatabaseTableTrait: TemporalDatabaseTableTrait[A]) = {
    e.toTupleReference(temporalDatabaseTableTrait)
  }

  //discard evidence to save memory
  def withoutEvidenceSets = FactMergeabilityGraph(edges
    .map(e => FactMergeabilityGraphEdge(e.tupleReferenceA,e.tupleReferenceB,e.evidence,None)),graphConfig)

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
        AssociationGraphEdge(keyList(0),keyList(1),summedEvidence,evidenceMultiSet)
      }}
    AssociationMergeabilityGraph(tableGraphEdges,graphConfig)
  }


  def idSet:Set[AssociationIdentifier] = edges.flatMap(e => Set[AssociationIdentifier](e.tupleReferenceA.associationID,e.tupleReferenceB.associationID)).toSet

  def writeToStandardFile() = {
    toJsonFile(FactMergeabilityGraph.getFieldLineageMergeabilityGraphFile(idSet,graphConfig))
  }

}
object FactMergeabilityGraph extends JsonReadable[FactMergeabilityGraph] with StrictLogging{

  def loadComponent(componentFile: File,subdomain:String,graphConfig: GraphConfig) = {
    val inputTables = Source.fromFile(componentFile)
      .getLines()
      .toIndexedSeq
      .map(stringID => {
        val id = AssociationIdentifier.fromCompositeID(stringID)
        val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)
        (id,table)
      }).toMap
    val fieldLineageMergeabilityGraph = FactMergeabilityGraph.loadSubGraph(inputTables.keySet,subdomain,graphConfig)
    fieldLineageMergeabilityGraph
  }

  def getAllConnectedComponentFiles(subdomain:String) = {
    new File(CONNECTED_COMPONENT_DIR(subdomain)).listFiles()
  }

  def idsFromFilename(f: File) = {
    val tokens = f.getName.split(";")
    val tokensFinal = tokens.slice(0,tokens.size-1) ++ Seq(tokens.last.replace(".json",""))
    tokensFinal.map(AssociationIdentifier.fromCompositeID(_)).toSet
  }

  def loadSubGraph(inputTableIDs: Set[AssociationIdentifier], subdomain:String,graphConfig: GraphConfig, discardEvidenceSet:Boolean = true) = {
    val files = getFieldLineageMergeabilityFiles(subdomain,graphConfig)
    val subGraphEdges = files
      .filter(f => {
        val idsInFile = idsFromFilename(f)
        idsInFile.forall(inputTableIDs.contains(_))
      })
      .flatMap(f => fromJsonFile(f.getAbsolutePath).withoutEvidenceSets.edges)
    FactMergeabilityGraph(subGraphEdges,graphConfig)
  }

  def readFieldLineageMergeabilityGraphAndAggregateToTableGraph(subdomain:String,graphConfig: GraphConfig, fileCountLimit:Int = Integer.MAX_VALUE) = {
    var count = 0
    val allEdges = getFieldLineageMergeabilityFiles(subdomain,graphConfig)
      .take(fileCountLimit)
      .toIndexedSeq
      .flatMap(f => {
        val tg = fromJsonFile(f.getAbsolutePath).transformToTableGraph
        count +=1
        if(count%100==0)
          logger.debug(s"Read $count files")
        tg.edges
      })
    AssociationMergeabilityGraph(allEdges,graphConfig)
  }

  def readAllBipartiteGraphs(subdomain:String,graphConfig: GraphConfig) = {
    val allEdges = getFieldLineageMergeabilityFiles(subdomain,graphConfig:GraphConfig)
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
    FactMergeabilityGraph(allEdges,graphConfig)
  }

  def readFromStandardFile(ids:Set[AssociationIdentifier],graphConfig: GraphConfig) = {
    fromJsonFile(getFieldLineageMergeabilityGraphFile(ids,graphConfig).getAbsolutePath)
  }

  def getFieldLineageMergeabilityFiles(subdomain:String,graphConfig: GraphConfig) = {
    new File(FIELD_LINEAGE_MERGEABILITY_GRAPH_DIR(subdomain,graphConfig)).listFiles()
  }

  def getFieldLineageMergeabilityGraphFile(ids: Set[AssociationIdentifier],graphConfig: GraphConfig): File = {
    val idString = ids.toIndexedSeq.map(_.compositeID).sorted.mkString(";")
    val subdomain = ids.map(_.subdomain).toSet
    if(subdomain.size!=1)
      println()
    assert(subdomain.size==1)
    createParentDirs(new File(FIELD_LINEAGE_MERGEABILITY_GRAPH_DIR(subdomain.head,graphConfig) + s"/" + idString + ".json"))
  }

}
