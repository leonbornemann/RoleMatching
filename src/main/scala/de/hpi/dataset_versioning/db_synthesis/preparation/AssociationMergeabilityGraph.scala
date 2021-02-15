package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.preparation.AssociationMergeabilityGraph.getAssociationMergeabilityGraphFile
import de.hpi.dataset_versioning.io.DBSynthesis_IOService.{ASSOCIATIONS_MERGEABILITY_GRAPH_DIR, ASSOCIATIONS_MERGEABILITY_SINGLE_EDGE_DIR, OPTIMIZATION_INPUT_DIR, createParentDirs}
import de.hpi.dataset_versioning.util.{MathUtil, TableFormatter}
import scalax.collection.edge.{WLkUnDiEdge, WUnDiEdge}
import scalax.collection.immutable.Graph

import java.io.File

case class AssociationMergeabilityGraph(edges: IndexedSeq[AssociationMergeabilityGraphEdge]) extends JsonWritable[AssociationMergeabilityGraph]{

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
    val edgesNew = edges
      .map(e => AssociationMergeabilityGraphEdge(e.v1,e.v2,e.summedEvidence,e.evidenceMultiSet.filter(t => p(t._1,t._2))))
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
    val sizes = traverser.map(c =>c.nodes.size)
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
    val asWundiedges = edges.map(e => WLkUnDiEdge(e.v1, e.v2)(e.summedEvidence,e.summedEvidence))
    val nodes = edges.flatMap(e => Set(e.v1,e.v2)).toSet
    val graph = Graph.from(nodes,asWundiedges)
    graph

  }

  def writeToStandardFile(subdomain:String) = {
    toJsonFile(getAssociationMergeabilityGraphFile(subdomain))
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
