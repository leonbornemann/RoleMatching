package de.hpi.dataset_versioning.data.change

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.DatasetInfo
import de.hpi.dataset_versioning.io.IOService

object ChangeExportMain extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val singleID = if(args.size==3) Some(args(2)) else None
  val exporter = new ChangeExporter()
  if(singleID.isDefined){
    logger.debug(s"Exporting all changes for single id ${singleID.get}")
    exporter.exportAllChanges(singleID.get)
  } else{
    logger.debug(s"Exporting all changes for all ids in ${subdomain}")
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
    subdomainIds.foreach(id => {
      logger.debug(s"Exporting all changes for $id")
      exporter.exportAllChanges(id)
    })
  }

}
