package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
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
    println("#Vertices:" + graph.toScalaGraph.nodes.size)
    println("#Edges:" + graph.toScalaGraph.edges.size)
    println("Evidence Sum:" + graph.toScalaGraph.edges.toIndexedSeq.map(_.weight).sum)
  }

  graphFiltered.printComponentSizeHistogram()
  //extreme filter:
  println("--------------------------------------------------------------------------------------")
  println("Filter null")
  println("--------------------------------------------------------------------------------------")
  val graphWithoutNull = graphRead.filterGraphEdges((t,_) => t.prev != null && t.after!=null)
  graphWithoutNull.printComponentSizeHistogram()
  printGraphInfo(graphWithoutNull)
  //print evidence sums:
  println("--------------------------------------------------------------------------------------")
  println("Filter _R")
  println("--------------------------------------------------------------------------------------")
  val graphWithOutRowDelete = graphRead.filterGraphEdges((t,_) => t.prev != ReservedChangeValues.NOT_EXISTANT_ROW && t.after!=ReservedChangeValues.NOT_EXISTANT_ROW)
  graphWithOutRowDelete.printComponentSizeHistogram()
  printGraphInfo(graphWithOutRowDelete)
  println("--------------------------------------------------------------------------------------")
  println("Filter null and _R")
  println("--------------------------------------------------------------------------------------")

  //fragment 1:
  val toFilter = Set[Any](null,ReservedChangeValues.NOT_EXISTANT_ROW)
  val sum = graphRead.edges.flatMap(e => e.evidenceMultiSet.filter(t => toFilter.contains(t._1.prev) || toFilter.contains(t._1.after))).map(_._2).sum
  println(sum)
  //fragment 2:
  graphRead.edges.forall(e => e.evidenceMultiSet.map(_._2).sum == e.summedEvidence)


  val graphWithOutNullAndRowDelete = graphRead.filterGraphEdges((t,_) => ! toFilter.contains(t.after) && !toFilter.contains(t.prev))
  graphWithOutNullAndRowDelete.printComponentSizeHistogram()
  printGraphInfo(graphWithOutNullAndRowDelete)
  println("--------------------------------------------------------------------------------------")
  println("IDF-Scores:")
  val idfScores = graphRead.idfScores
    .toIndexedSeq.sortBy(_._2)
    .take(100)
    .map(t => (Seq(t._1,f"${t._2}%.3f")))
  val header = Seq("Transition","IDF-Score")
  TableFormatter.printTable(header,idfScores)
}
