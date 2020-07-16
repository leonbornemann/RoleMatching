package de.hpi.dataset_versioning.db_synthesis.top_down

import de.hpi.dataset_versioning.io.IOService

object TopDownMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val topDown = new TopDown(subdomain)
  topDown.synthesizeDatabase()
}
