package de.hpi.socrata.change

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.io.Socrata_IOService
import de.hpi.socrata.metadata.custom.DatasetInfo

object ChangeExportMain extends App with StrictLogging{

  Socrata_IOService.socrataDir = args(0)
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
