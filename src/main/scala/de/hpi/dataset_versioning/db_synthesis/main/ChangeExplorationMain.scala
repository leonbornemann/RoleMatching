package de.hpi.dataset_versioning.db_synthesis.main

import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.change.ChangeExportMain.subdomain
import de.hpi.dataset_versioning.db_synthesis.decomposition.DatasetInfo
import de.hpi.dataset_versioning.io.IOService

object ChangeExplorationMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = subDomainInfo(subdomain)
    .map(_.id)
  val ccs = ChangeCube.loadAllChanges(subdomainIds)
  //ccs.flatMap(cc => (cc.datasetID,cc.allChanges.map(_.)))
}
