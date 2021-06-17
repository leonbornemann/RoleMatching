package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import de.hpi.tfm.data.wikipedia.infobox.fact_merging.EdgeAnalysisMain.args

import java.io.File
import java.time.LocalDate

object GraphFilterMain extends App {
  val matchFile = new File(args(0))
  val resultFile = new File(args(1))
  val timeStart = LocalDate.parse(args(2))
  val endDateTrainPhases = args(3).split(";").map(LocalDate.parse(_)).toIndexedSeq
  val timeEnd = LocalDate.parse(args(4))


  assert(false) //TODO:also needs to create TF-IDF

}
