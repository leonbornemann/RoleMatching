package de.hpi.dataset_versioning.db_synthesis.graph.association

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.util.TableFormatter

object AssociationMergeabilityGraphExplorationMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val graphRead = AssociationMergeabilityGraph.readFromStandardFile(subdomain)
  graphRead.printComponentSizeHistogram()
  graphRead.detailedComponentPrint()
  val graph = graphRead.toScalaGraph
  printGraphInfo(graphRead)
  println("--------------------------------------------------------")
  graphRead.printTopTransitionCounts(2000)
  printGraphInfo(graphRead)
  graphRead.printComponentSizeHistogram()

  private def printGraphInfo(graph: AssociationMergeabilityGraph) = {
    println("#Vertices:" + graph.toScalaGraph.nodes.size)
    println("#Edges:" + graph.toScalaGraph.edges.size)
    println("Evidence Sum:" + graph.toScalaGraph.edges.toIndexedSeq.map(_.weight).sum)
  }

  println("--------------------------------------------------------------------------------------")
  println("IDF-Scores:")
  val idfScores = graphRead.idfScores
    .toIndexedSeq.sortBy(_._2)
    .take(100)
    .map(t => (Seq(t._1, f"${t._2}%.3f")))
  val header = Seq("Transition", "IDF-Score")
  TableFormatter.printTable(header, idfScores)
}
