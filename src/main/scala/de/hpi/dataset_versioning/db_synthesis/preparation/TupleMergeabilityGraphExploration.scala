package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object TupleMergeabilityGraphExploration extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val graph = FieldLineageMergeabilityGraph.readTableGraph(subdomain)
    .sortBy(-_._2)
  val associationMergeabilityGraph = AssociationMergeabilityGraph(graph.map{case (associations,summedEvidence) => {
    assert(associations.size == 2)
    val list = associations.toList
    AssociationMergeabilityGraphEdge(list(0), list(1), summedEvidence)
  }
  })
  associationMergeabilityGraph.writeToStandardFile(subdomain)


}
