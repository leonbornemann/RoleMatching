package de.hpi.role_matching.cbrm.compatibility_graph.representation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.data_preparation.socrata.io.Socrata_Synthesis_IOService.{CONNECTED_ASSOCIATION_COMPONENT_DIR, FIELD_LINEAGE_MERGEABILITY_GRAPH_DIR, createParentDirs}
import de.hpi.data_preparation.socrata.tfmp_input.association.AssociationIdentifier
import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.CompatibilityGraphEdge

import java.io.File
import scala.io.Source

case class RoleCompatibilityGraph(edges: IndexedSeq[CompatibilityGraphEdge], graphConfig: GraphConfig) extends JsonWritable[RoleCompatibilityGraph]{

  //discard evidence to save memory
  def withoutEvidenceSets = RoleCompatibilityGraph(edges
    .map(e => CompatibilityGraphEdge(e.tupleReferenceA,e.tupleReferenceB,e.evidence,None)),graphConfig)


  def idSet:Set[AssociationIdentifier] = edges.flatMap(e => Set[AssociationIdentifier](e.tupleReferenceA.associationID,e.tupleReferenceB.associationID)).toSet

  def writeToStandardFile() = {
    toJsonFile(RoleCompatibilityGraph.getFieldLineageMergeabilityGraphFile(idSet,graphConfig))
  }

}
object RoleCompatibilityGraph extends JsonReadable[RoleCompatibilityGraph] with StrictLogging{

  def loadCompleteGraph(subdomain:String,graphConfig: GraphConfig) = {
    val allEdges = getFieldLineageMergeabilityFiles(subdomain,graphConfig)
      .flatMap(f => fromJsonFile(f.getAbsolutePath).edges)
    RoleCompatibilityGraph(allEdges,graphConfig)
  }

  def loadComponent(componentFile: File,subdomain:String,graphConfig: GraphConfig) = {
    val inputTables = Source.fromFile(componentFile)
      .getLines()
      .toIndexedSeq
      .map(stringID => {
        val id = AssociationIdentifier.fromCompositeID(stringID)
        val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)
        (id,table)
      }).toMap
    val fieldLineageMergeabilityGraph = RoleCompatibilityGraph.loadSubGraph(inputTables.keySet,subdomain,graphConfig)
    fieldLineageMergeabilityGraph
  }

  def getAllConnectedComponentFiles(subdomain:String,graphConfig: GraphConfig) = {
    new File(CONNECTED_ASSOCIATION_COMPONENT_DIR(subdomain,graphConfig)).listFiles()
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
    RoleCompatibilityGraph(subGraphEdges,graphConfig)
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
    RoleCompatibilityGraph(allEdges,graphConfig)
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
