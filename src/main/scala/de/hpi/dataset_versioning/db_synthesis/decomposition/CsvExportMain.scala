package de.hpi.dataset_versioning.db_synthesis.decomposition

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.io.IOService

object CsvExportMain extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val subdomain = args(1) //org.cityofchicago
  val byDomain = DatasetInfo.readDatasetInfoBySubDomain
  val toExport = byDomain(subdomain)
  logger.debug(s"Found ${toExport.size} datasets")
  toExport.filter(_.isNotDeletedAtFinalTimestamp)
    .foreach(dsInfo => {
      try {
        val ds = IOService.loadSimplifiedRelationalDataset(DatasetInstance(dsInfo.id, dsInfo.getLatestVersion))
        ds.toCSV(IOService.getSimplifiedCSVExportFile(DatasetInstance(dsInfo.id,dsInfo.getLatestVersion)))
        logger.debug(s"Finsihed Exporting ${ds.id} (version ${ds.version})")
      } catch {
        case _:Throwable => logger.debug(s"exception wile trying to load ${dsInfo.id} (version ${dsInfo.getLatestVersion})")
      }
    })
}
