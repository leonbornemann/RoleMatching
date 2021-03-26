package de.hpi.tfm.oneshot

import de.hpi.tfm.io.IOService

import java.io.{File, PrintWriter}

object DefaultColumnOrder extends App {

//  IOService.socrataDir = args(0)
//  private val date = LocalDate.parse("2019-11-01", IOService.dateTimeFormatter)
//  IOService.cacheMetadata(date)
//  val md = IOService.cachedMetadata(date)("2b3m-wnm2")
//    md
//    .resource
//    .columns_name
//    .foreach(println(_))
//  println()

  val files = new File("/san2/data/data-prep/socrata-csv/data")
    .listFiles()
    .filter(_.getName.endsWith(".csv?"))
  val defaultColumnOrderFile = "/san2/data/change-exploration/socrata/db_synthesis/generalMetadata/defaultColumnOrder.csv"
  val pr = new PrintWriter(defaultColumnOrderFile)
  val outputLines = files.foreach(f => pr.println(s"${IOService.filenameToID(f)},${firstLine(f).get}"))
  pr.close()



  def firstLine(f: java.io.File): Option[String] = {
    val src = io.Source.fromFile(f)
    try {
      src.getLines.find(_ => true)
    } finally {
      src.close()
    }
  }
}
