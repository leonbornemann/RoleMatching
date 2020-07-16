package de.hpi.dataset_versioning.db_synthesis.bottom_up

import java.io.File

object FieldLineageExploration extends App {

  val f = new File("/home/leon/Desktop/fieldLineageEquality.json")
  val values = FieldLineageOccurrenceInfo.fromJsonObjectPerLineFile(f.getAbsolutePath)
  values.groupBy(_.cols.size>1)
  println(values.size)
}
