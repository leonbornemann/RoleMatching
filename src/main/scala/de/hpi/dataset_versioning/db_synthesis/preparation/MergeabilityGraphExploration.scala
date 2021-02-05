package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object MergeabilityGraphExploration extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val graph = FieldLineageMergeabilityGraph.readTableGraph(subdomain)
    .sortBy(-_._2)
  graph.take(500)
    .foreach(t => println(t))
  println("Last ----------------------------------------------------------------------")
  graph.takeRight(500)
    .foreach(t => println(t))
  println("Size:---------------------------------------------------------")
  println(graph.size)

}
