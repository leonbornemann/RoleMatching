package de.hpi.tfm.compatibility.graph.association

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.io.IOService

import java.time.LocalDate

//combines graphs from single edge files
object AssociationMergeabilityGraphAggregationMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val minEvidence = args(2).toInt
  val timeRangeStart = LocalDate.parse(args(3))
  val timeRangeEnd = LocalDate.parse(args(4))
  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
  val associationMergeabilityGraph = AssociationMergeabilityGraph.readFromSingleEdgeFiles(subdomain,graphConfig)
  associationMergeabilityGraph.writeToStandardFile(subdomain,graphConfig)


}
