package de.hpi.dataset_versioning.db_synthesis.bottom_up

import de.hpi.dataset_versioning.io.IOService

object BottomUpMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val bottomUp = new BottomUpDBSynthesis(subdomain)
  bottomUp.synthesize()
}
