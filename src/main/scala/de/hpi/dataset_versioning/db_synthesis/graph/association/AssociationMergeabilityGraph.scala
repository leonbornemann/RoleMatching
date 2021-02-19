package de.hpi.dataset_versioning.db_synthesis.graph.association

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.metadata.custom.DatasetMetaInfo
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.ValueTransition
import de.hpi.dataset_versioning.io.DBSynthesis_IOService.{ASSOCIATIONS_MERGEABILITY_GRAPH_DIR, ASSOCIATIONS_MERGEABILITY_SINGLE_EDGE_DIR, createParentDirs}
import de.hpi.dataset_versioning.util.{MathUtil, TableFormatter}
import scalax.collection.edge.WLkUnDiEdge
import scalax.collection.immutable.Graph

import java.io.File

case class AssociationMergeabilityGraph(edges: IndexedSeq[AssociationMergeabilityGraphEdge]) extends JsonWritable[AssociationMergeabilityGraph]{

  def detailedComponentPrint() = {
    val subdomain = edges.head.v1.subdomain
    val metadata = DatasetMetaInfo.readAll(subdomain)
      .map(e => (e.id,e))
      .toMap
    val componentsOrdered = toScalaGraph.componentTraverser()
      .toIndexedSeq
      .sortBy(_.nodes.size)
    componentsOrdered.foreach(c => {
      println("-------------------------------------------------------------------------------------------------------")
      println("-------------------------------------------------------------------------------------------------------")
      val componentEdges = Set() ++ c.edges //weird hack because of a weird bug!
      val componentNodes = Set() ++ c.nodes
      val totalEvidence = componentEdges.toIndexedSeq.map(_.weight).sum.toInt
      println(s"SIZE: ${componentNodes.size} EVIDENCE: $totalEvidence")
      val ids = componentNodes.map(_.value)
      val byViewID = ids.groupBy(_.viewID)
      println("VERTICES: ")
      byViewID.foreach{case (v,aids) => {
        val colString = "[" + aids.map(dttID => metadata(v).associationInfoByID(dttID).colName).mkString(" ; ") + "]"
        println(s"${v}( ${metadata(v).name} ): $colString")
      }}
      val edgeObjects = componentEdges.map(_.label.asInstanceOf[AssociationMergeabilityGraphEdge])
      val evidences = edgeObjects.toIndexedSeq.flatMap(_.evidenceMultiSet.toIndexedSeq)
        .groupMap(_._1)(_._2)
        .map(t => (t._1,t._2.sum))
      if(evidences.values.sum != totalEvidence) {
        val labels = toScalaGraph.edges.toIndexedSeq.map(e => e.label.asInstanceOf[AssociationMergeabilityGraphEdge])
        println()
        println()
      }
      assert(evidences.values.sum == totalEvidence )
      val byContainsRowDelete = evidences.groupBy{case (t,count) => t.after==ReservedChangeValues.NOT_EXISTANT_ROW || t.prev == ReservedChangeValues.NOT_EXISTANT_ROW}

      val containsRowDelete = byContainsRowDelete.getOrElse(true, Map())
      val containsRowDeleteTop5 =  "[" + getTop5(containsRowDelete).mkString(";")  + "]"
      val containsNoRowDelete = byContainsRowDelete.getOrElse(false, Map())
      val containsNoRowDeleteTop5 = "[" + getTop5(containsNoRowDelete).mkString(";") + "]"
      println("-")
      println(s"With Row Delete (${containsRowDelete.map(_._2).sum}): $containsRowDeleteTop5")
      println(s"Without Row Delete (${containsNoRowDelete.map(_._2).sum}): $containsNoRowDeleteTop5")
      println("-------------------------------------------------------------------------------------------------------")
      println("-------------------------------------------------------------------------------------------------------")
    })
  }


  private def getTop5(map: Map[ValueTransition, Int]) = {
    map.toIndexedSeq
      .sortBy(-_._2)
      .take(5)
      .map(t => (t._1.toShortString, t._2))
  }

  def writeToSingleEdgeFile(filename: String, subdomain:String) = {
    val singleEdgeDir = AssociationMergeabilityGraph.getSingleEdgeDir(subdomain)
    val file = createParentDirs(new File(singleEdgeDir.getAbsolutePath + s"/$filename"))
    toJsonFile(file)
  }

  assert(edges.groupBy(e => Set(e.v1,e.v2)).forall(_._2.size==1))

  def idfScores = {
    val documentCount = edges.size
    val transitionToEdgeCounts = edges
      .flatMap(e => e.evidenceMultiSet.map(t => (t._1,e)))
      .groupMap(_._1)(_._2)
      .map(t => (t._1,t._2.toSet.size))
    transitionToEdgeCounts.map{case (t,n) => (t,MathUtil.log2(documentCount / n.toDouble))}
  }

  def getTopTransitionCounts(n: Int) = {
    val table = edges.flatMap(_.evidenceMultiSet)
      .groupBy(_._1)
      .map{case (k,v) => (k,v.map(_._2).sum,v.size)}
      .toIndexedSeq
      .sortBy(-_._2)
      .take(n)
    table
  }


  def filterGraphEdges(p : ((ValueTransition,Int) => Boolean)) = {
    //TODO: recompute summedEvidence here:
    val edgesNew = edges
      .map(e => {
        val newFilteredEvidenceMultiSet = e.evidenceMultiSet.filter(t => p(t._1,t._2))
        val newSUmmedEvidence = newFilteredEvidenceMultiSet.map(_._2).sum
        AssociationMergeabilityGraphEdge(e.v1,e.v2,newSUmmedEvidence,newFilteredEvidenceMultiSet)
      })
      .filter(_.evidenceMultiSet.size>0)
    AssociationMergeabilityGraph(edgesNew)
  }


  def printTopTransitionCounts(n:Int) = {
    val table = getTopTransitionCounts(n)
      .map(t => Seq(t._1,t._2,t._3))
    TableFormatter.printTable(Seq("Transition","#Occurrences","#OccurrencesPerPair"),table)
  }

  def printComponentSizeHistogram() = {
    val graph = toScalaGraph
    val traverser = graph.componentTraverser()
    val sizes = traverser.map(c => (Set() ++ c.nodes).size)
    val header = IndexedSeq("Component Size","#Occurrences")
    val hist = sizes.groupBy(identity)
      .map{case (k,v) => (k,v.size)}
      .toIndexedSeq
      .sortBy(_._1)
      .map(t => Seq(t._1,t._2))
    TableFormatter.printTable(header,hist)
    //println(TableFormatter.format(Seq(header) ++ hist))
  }


  def toScalaGraph = {
    val asWundiedges = edges.map(e => WLkUnDiEdge(e.v1, e.v2)(e.summedEvidence,e))
    val nodes = edges.flatMap(e => Set(e.v1,e.v2)).toSet
    val graph = Graph.from(nodes,asWundiedges)
    graph

  }

  def writeToStandardFile(subdomain:String) = {
    toJsonFile(AssociationMergeabilityGraph.getAssociationMergeabilityGraphFile(subdomain))
  }

}
object AssociationMergeabilityGraph extends JsonReadable[AssociationMergeabilityGraph]{
  def readFromSingleEdgeFiles(subdomain: String) = {
    val edges = getSingleEdgeDir(subdomain)
      .listFiles()
      .flatMap(f => fromJsonFile(f.getAbsolutePath).edges)
      .toIndexedSeq
    AssociationMergeabilityGraph(edges)
  }

  def getSingleEdgeDir(subdomain: String) = createParentDirs(new File(ASSOCIATIONS_MERGEABILITY_SINGLE_EDGE_DIR + s"/$subdomain/"))

  def readFromStandardFile(subdomain:String) = {
    fromJsonFile(getAssociationMergeabilityGraphFile(subdomain).getAbsolutePath)
  }


  def getAssociationMergeabilityGraphFile(subdomain: String) = {
    val file = new File(s"$ASSOCIATIONS_MERGEABILITY_GRAPH_DIR/$subdomain/associationMergeabilityGraph.json")
    createParentDirs(file)
  }

}
