package de.hpi.dataset_versioning.data.parser

import java.io.File
import java.time.LocalDate

object JsonParsingMain extends App {
  val dirWithUncompressedFiles = args(0)
  new JsonDataParser().parseAllJson(new File(dirWithUncompressedFiles),LocalDate.now())
}
