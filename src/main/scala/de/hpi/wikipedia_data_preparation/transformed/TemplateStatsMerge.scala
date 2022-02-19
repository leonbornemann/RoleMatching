package de.hpi.wikipedia_data_preparation.transformed

import java.io.File

object TemplateStatsMerge extends App {
  val inputDir = new File(args(0))
  val stats = inputDir.listFiles().map(f => TemplateStats.fromJsonFile(f.getAbsolutePath))
    .reduce(_ add _)
  stats.nameToCount.toIndexedSeq
    .sortBy(-_._2)
    .take(500)
    .foreach(t => println(t._1 + " : " + t._2))

}
