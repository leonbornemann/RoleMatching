package de.hpi.role_matching.evaluation

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.time.LocalDate
import scala.util.Random

object OneshotTestOfDensity extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val trainTimeEnd = LocalDate.parse(GLOBAL_CONFIG.finalWikipediaTrainTimeENd)
  val rs = Roleset.fromJsonFile("/home/leon/data/dataset_versioning/finalExperiments/nodecayrolesets_wikipedia/politics.json",Some(0.8),Some(trainTimeEnd),Some(new Random(13)))
  //println(rs)

}
