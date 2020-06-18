package de.hpi.dataset_versioning.data.change

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.decomposition.DatasetInfo
import de.hpi.dataset_versioning.io.IOService

object ChangeExportMain extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
  val exporter = new ChangeExporter()
  subdomainIds.foreach(id => {
    logger.debug(s"Exporting all changes for $id")
    exporter.exportAllChanges(id)
  })
}
