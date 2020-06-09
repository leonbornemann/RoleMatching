package de.hpi.dataset_versioning.data.exploration

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.data.OldLoadedRelationalDataset
import de.hpi.dataset_versioning.data.parser.JsonDataParser
import de.hpi.dataset_versioning.io.IOService

object SingleDatasetRead extends App {

  val path = new File(args(0))
  val jsonParser = new JsonDataParser
  val a = jsonParser.tryParseJsonFile(path,"id",LocalDate.MIN)
  println(a.size)

}
