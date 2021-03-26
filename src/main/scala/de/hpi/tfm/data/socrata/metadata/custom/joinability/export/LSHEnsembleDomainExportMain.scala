package de.hpi.tfm.data.socrata.metadata.custom.joinability.`export`

import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate

object LSHEnsembleDomainExportMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
  val startVersion = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val endVersion = LocalDate.parse(args(2),IOService.dateTimeFormatter)
  val outDir = new File(args(3))
  val reExportStartVersion = args(4).toBoolean
  new LSHEnsembleDomainExporter().export(startVersion,endVersion,outDir,reExportStartVersion)

}
