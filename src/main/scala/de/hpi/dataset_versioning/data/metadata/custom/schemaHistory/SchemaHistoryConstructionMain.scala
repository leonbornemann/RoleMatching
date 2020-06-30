package de.hpi.dataset_versioning.data.metadata.custom.schemaHistory

import de.hpi.dataset_versioning.io.IOService

object SchemaHistoryConstructionMain extends App {

  IOService.socrataDir = args(0)
  SchemaHistory.exportAllSchemaHistories()
}
