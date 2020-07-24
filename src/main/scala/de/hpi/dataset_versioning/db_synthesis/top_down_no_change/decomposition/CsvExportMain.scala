package de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.io.IOService

object CsvExportMain extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val subdomain = args(1) //org.cityofchicago
  val byDomain = DatasetInfo.readDatasetInfoBySubDomain
  val toExport = byDomain(subdomain)
  val versionHistory = DatasetVersionHistory.load()
      .map(vh => (vh.id,vh))
      .toMap
  toExport.foreach(dsInfo => {
    val dsID = dsInfo.id
    val vh = versionHistory(dsID)
    vh.versionsWithChanges.foreach(v => {
      try {
        val ds = IOService.loadSimplifiedRelationalDataset(DatasetInstance(dsID, v))
        ds.toCSV(IOService.getSimplifiedCSVExportFile(DatasetInstance(dsInfo.id, v), subdomain)) //TODO: column changes (?) - name ist still enough as we know the timestamp
      }  catch {
        case _:Throwable => logger.debug(s"exception wile trying to load ${dsInfo.id} (version $v)")
      }
    })
    logger.debug(s"Finsihed Exporting all versions of ${dsID}")
  })
}
