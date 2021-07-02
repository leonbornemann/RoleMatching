package de.hpi.socrata.metadata.custom.schemaHistory

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.io.Socrata_IOService
import de.hpi.socrata.metadata.custom.DatasetInfo

object TemporalSchemaExport extends App with StrictLogging{
  Socrata_IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  subdomainIds.foreach(id => {
    logger.debug(s"Loading changes for $id")
    val schema = TemporalSchema.readFromTemporalTable(id)
    schema.writeToStandardFile()
  })
}
