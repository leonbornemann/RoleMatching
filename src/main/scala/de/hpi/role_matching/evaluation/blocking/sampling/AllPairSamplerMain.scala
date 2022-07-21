package de.hpi.role_matching.evaluation.blocking.sampling

import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate

object AllPairSamplerMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val rolesetDir = new File(args(1))
  val outputDir = args(2)
  val trainTimeEnd = LocalDate.parse(args(3))
  val sampleCount = args(4).toInt
  val seed = 13
  new SimpleAllPairSampler(rolesetDir, outputDir, trainTimeEnd, sampleCount, seed).runSampling

}
