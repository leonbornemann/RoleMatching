package de.hpi.tfm.data.socrata.crawl

import java.io.File
import java.time.LocalDate

object SocrataCrawlMain extends App {
  val metadataResultDir = args(0) + "/" + LocalDate.now() + "/"
  val urlFile = args(1) + "/" + LocalDate.now() + "/"
  new File(metadataResultDir).mkdirs()
  new File(urlFile).mkdirs()
  new SocrataMetadataCrawler(metadataResultDir).crawl(urlFile)
}
