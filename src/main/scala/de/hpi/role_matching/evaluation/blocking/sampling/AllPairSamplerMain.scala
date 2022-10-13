package de.hpi.role_matching.evaluation.blocking.sampling

import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate

object AllPairSamplerMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val rolesetDir = new File(args(0))
  val outputDir = args(1)
  new File(outputDir).mkdirs()
  val trainTimeEnd = LocalDate.parse(GLOBAL_CONFIG.finalWikipediaTrainTimeENd)
  val sampleCount = args(2).toInt
  val seed = 13
  val jsonOnly = args(3).toBoolean
  new SimpleAllPairSampler(rolesetDir, outputDir, trainTimeEnd, sampleCount, seed,jsonOnly).runSampling

}
