package de.hpi.dataset_versioning.data.change.temporal_tables

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.oneshot.ScriptMain.args

object TemporalColumnExporter extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  subdomainIds.foreach(id => {
    logger.debug(s"processing $id")
    val tt = TemporalTable.load(id)
    val cols = tt.getTemporalColumns()
    cols.foreach(col => col.writeToStandardFile())
  })
}
