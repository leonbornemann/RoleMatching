package de.hpi.tfm.compatibility.graph.association.connected_component

import de.hpi.tfm.io.IOService

object ConnectedComponentCreationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  new AssociationConnectedComponentCreator(subdomain).create()
}
