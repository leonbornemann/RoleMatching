package de.hpi.tfm.compatibility.graph.association

import de.hpi.tfm.io.IOService

//combines graphs from single edge files
object AssociationMergeabilityGraphAggregationMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val associationMergeabilityGraph = AssociationMergeabilityGraph.readFromSingleEdgeFiles(subdomain)
  associationMergeabilityGraph.writeToStandardFile(subdomain)


}
