package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object MergeabilityGraphExploration extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val graph = FieldLineageMergeabilityGraph.readAllBipartiteGraphs(subdomain)
  val tableGraphEdges = graph.transformToTableGraph
  tableGraphEdges
    .toIndexedSeq
    .sortBy(-_._2)
    .foreach(t => println(t))

}
