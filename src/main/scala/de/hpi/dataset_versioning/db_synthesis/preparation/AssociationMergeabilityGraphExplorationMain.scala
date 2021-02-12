package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.util.TableFormatter
import scalax.collection.Graph
import scalax.collection.edge.WLkUnDiEdge

object AssociationMergeabilityGraphExplorationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val graphRead = AssociationMergeabilityGraph.readFromStandardFile(subdomain)
  graphRead.printComponentSizeHistogram()
  val graph = graphRead.toScalaGraph
  printGraphInfo(graphRead)
  println("--------------------------------------------------------")
  graphRead.printTopTransitionCounts(20)
  val transitionsToFilter = graphRead.getTopTransitionCounts(2)
    .map(_._1)
    .toSet
  val graphFiltered = graphRead.filterGraphEdges((t,_) => !transitionsToFilter.contains(t))
  printGraphInfo(graphFiltered)

  private def printGraphInfo(graph:AssociationMergeabilityGraph) = {
    println(graph.toScalaGraph.edges.toIndexedSeq.map(_.weight).sum)
    println(graph.toScalaGraph.nodes.size)
    println(graph.toScalaGraph.edges.size)
  }

  graphFiltered.printComponentSizeHistogram()
  //extreme filter:
  println("--------------------------------------------------------------------------------------")
  val graphExtremeFiltered = graphRead.filterGraphEdges((t,_) => t.prev != null)
  graphExtremeFiltered.printComponentSizeHistogram()
  printGraphInfo(graphExtremeFiltered)
  //print evidence sums:
  println("--------------------------------------------------------------------------------------")
  val evidenceFilteredExtreme = graphExtremeFiltered.edges.map(_.summedEvidence).sum
  val evidenceSumFiltered = graphFiltered.edges.map(_.summedEvidence).sum
  val evidenceSum = graphRead.edges.map(_.summedEvidence).sum
  println(evidenceFilteredExtreme)
  println(evidenceSumFiltered)
  println(evidenceSum)

  println("--------------------------------------------------------------------------------------")
  println("IDF-Scores:")
  val idfScores = graphRead.idfScores
    .toIndexedSeq.sortBy(_._2)
    .take(100)
    .map(t => (Seq(t._1,f"${t._2}%.3f")))
  val header = Seq("Transition","IDF-Score")
  TableFormatter.printTable(header,idfScores)
}
