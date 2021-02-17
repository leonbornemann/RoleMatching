package de.hpi.dataset_versioning.data.metadata.custom

import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.io.IOService

object DatasetMetaInfoCreationMain extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  new DatasetMetaInfoCreator(subdomain).create()
}
