package de.hpi.socrata.`export`

import de.hpi.socrata.io.Socrata_IOService
import de.hpi.socrata.metadata.custom.DatasetInfo

import java.io.File
import java.time.LocalDate

object SimplifiedInputExportMain extends App {
  Socrata_IOService.socrataDir = args(0)
  val subdomain = args(1)
  val ids = DatasetInfo.readDatasetInfoBySubDomain(subdomain)
    .map(_.id)
  val identifiedLineageDir = new File(args(2))
  val trainTimeEnd = LocalDate.parse(args(3))
  assert(identifiedLineageDir.getParentFile.exists())
  ids.foreach(id => {
    val thisFile = new File(identifiedLineageDir.getAbsolutePath + "/" + s"$id.json")
    val exporter = new SimplifiedInputExporter(subdomain, id)
    exporter.exportAll(thisFile,trainTimeEnd)
  })
}
