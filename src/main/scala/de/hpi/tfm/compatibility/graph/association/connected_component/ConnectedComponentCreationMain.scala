package de.hpi.tfm.compatibility.graph.association.connected_component

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.bipartite.BipartiteFactMatchGraphCreationMain.args
import de.hpi.tfm.io.IOService

import java.time.LocalDate

object ConnectedComponentCreationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val minEvidence = args(2).toInt
  val timeRangeStart = LocalDate.parse(args(3))
  val timeRangeEnd = LocalDate.parse(args(4))
  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
  new AssociationConnectedComponentCreator(subdomain,graphConfig).create()
}
