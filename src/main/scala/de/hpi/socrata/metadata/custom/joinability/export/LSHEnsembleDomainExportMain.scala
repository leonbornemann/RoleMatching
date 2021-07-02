package de.hpi.socrata.metadata.custom.joinability.`export`

import de.hpi.socrata.io.Socrata_IOService

import java.io.File
import java.time.LocalDate

object LSHEnsembleDomainExportMain extends App {
  Socrata_IOService.socrataDir = args(0)
  Socrata_IOService.printSummary()
  val startVersion = LocalDate.parse(args(1),Socrata_IOService.dateTimeFormatter)
  val endVersion = LocalDate.parse(args(2),Socrata_IOService.dateTimeFormatter)
  val outDir = new File(args(3))
  val reExportStartVersion = args(4).toBoolean
  new LSHEnsembleDomainExporter().export(startVersion,endVersion,outDir,reExportStartVersion)

}
