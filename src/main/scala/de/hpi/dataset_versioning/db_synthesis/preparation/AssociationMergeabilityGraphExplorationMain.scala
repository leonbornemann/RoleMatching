package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.io.IOService

object AssociationMergeabilityGraphExplorationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val graphRead = AssociationMergeabilityGraph.readFromStandardFile(subdomain)
  val graph = graphRead.toScalaGraph
  println(graph.edges.toIndexedSeq.map(_.weight).sum)
  println(graph.nodes.size)
  println(graph.edges.size)
  val traverser = graph.componentTraverser()
  val sizes = traverser.map(c => {
    println(c.nodes.size,c.edges.map(_.weight).sum)
    c.nodes.size
  })
  println("----------------------------------------------------------------")
  sizes.groupBy(identity)
    .map{case (k,v) => (k,v.size)}
    .toIndexedSeq
    .sortBy(_._1)
    .foreach(println(_))
  println("--------------------------------------------------------------------------------------------------")
}
