package de.hpi.tfm.data.socrata.metadata.custom

import de.hpi.tfm.io.IOService

object DatasetMetaInfoCreationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  new DatasetMetaInfoCreator(subdomain).create()
}
