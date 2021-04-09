package de.hpi.tfm.compatibility.graph.fact

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.association.connected_component.AssociationConnectedComponentCreator
import de.hpi.tfm.compatibility.graph.association.connected_component.ConnectedComponentCreationMain.{graphConfig, subdomain}
import de.hpi.tfm.fact_merging.optimization.SimpleGreedyEdgeWeightOptimizationMain.args
import de.hpi.tfm.io.IOService

import java.time.LocalDate

object AssociationMergeabilityGraphCreationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val minEvidence = args(2).toInt
  val timeRangeStart = LocalDate.parse(args(3))
  val timeRangeEnd = LocalDate.parse(args(4))
  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
  val graph = FactMergeabilityGraph.loadCompleteGraph(subdomain,graphConfig)
  val associationGraph = graph.transformToAssociationGraph
  associationGraph.writeToStandardFile(subdomain,graphConfig)
  new AssociationConnectedComponentCreator(subdomain,graphConfig).create()

}
