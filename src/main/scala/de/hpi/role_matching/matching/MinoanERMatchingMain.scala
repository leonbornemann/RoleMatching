package de.hpi.role_matching.matching

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate

object MinoanERMatchingMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputDataPath = new File(args(0))
  val rolesetPath = args(1)
  val resultPath = new File(args(2))
  val trainTimeEnd = LocalDate.parse(GLOBAL_CONFIG.finalWikipediaTrainTimeENd)
  val rs = Roleset.fromJsonFile(rolesetPath)
  val minoanER = new MinoanERMatcher(new PrintWriter(resultPath),trainTimeEnd,rs,inputDataPath)

}
