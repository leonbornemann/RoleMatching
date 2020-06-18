package de.hpi.dataset_versioning.oneshot

import java.io.{File, PrintWriter}

import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

object DefaultColumnOrder extends App {

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
