package de.hpi.tfm.compatibility

import de.hpi.tfm.io.IOService

object NewCompatibilityGraphCreationMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)

}
