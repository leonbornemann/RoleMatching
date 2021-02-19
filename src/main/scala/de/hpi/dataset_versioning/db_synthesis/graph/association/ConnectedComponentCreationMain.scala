package de.hpi.dataset_versioning.db_synthesis.graph.association

import de.hpi.dataset_versioning.io.IOService

object ConnectedComponentCreationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  new AssociationConnectedComponentCreator(subdomain).create()
}
