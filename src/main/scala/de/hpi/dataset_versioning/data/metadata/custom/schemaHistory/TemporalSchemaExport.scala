package de.hpi.dataset_versioning.data.metadata.custom.schemaHistory

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.DatasetInfo
import de.hpi.dataset_versioning.io.IOService

object TemporalSchemaExport extends App with StrictLogging{
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  subdomainIds.foreach(id => {
    logger.debug(s"Loading changes for $id")
    val schemaBeforeWrite = TemporalSchema.readFromTemporalTable(id)
    schemaBeforeWrite.writeToStandardFile()
  })
}
