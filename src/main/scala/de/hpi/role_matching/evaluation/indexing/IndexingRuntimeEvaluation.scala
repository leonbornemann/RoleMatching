package de.hpi.role_matching.evaluation.indexing

import java.io.File
import scala.io.Source

object IndexingRuntimeEvaluation extends App {
  val resultDir = new File(args(0))
  val sum = resultDir
    .listFiles
    .flatMap(f => Source.fromFile(f).getLines())
    .toIndexedSeq
    .map(_.toInt)
    .sum
  println(resultDir.getName,sum)
}
