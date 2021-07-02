package de.hpi.socrata.metadata.custom

import de.hpi.socrata.io.Socrata_IOService

object DatasetMetaInfoCreationMain extends App {

  Socrata_IOService.socrataDir = args(0)
  val subdomain = args(1)
  new DatasetMetaInfoCreator(subdomain).create()
}
