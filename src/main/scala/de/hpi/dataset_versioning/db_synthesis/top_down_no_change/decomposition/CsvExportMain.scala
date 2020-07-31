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
        assert(ds.attributes.map(_.position.get) == (0 until ds.attributes.size))
        ds.toCSV(IOService.getSimplifiedCSVExportFile(DatasetInstance(dsInfo.id, v), subdomain))
      }  catch {
        case _:Throwable => logger.debug(s"exception wile trying to load ${dsInfo.id} (version $v)")
      }
    })
    logger.debug(s"Finsihed Exporting all versions of ${dsID}")
  })
}
