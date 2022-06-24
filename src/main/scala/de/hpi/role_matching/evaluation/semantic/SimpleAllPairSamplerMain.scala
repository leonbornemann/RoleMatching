package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate

object SimpleAllPairSamplerMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val rolesetDir = new File(args(1))
  val outputDir = args(2)
  val trainTimeEnd = LocalDate.parse(args(3))
  val sampleCount = args(4).toInt
  val minVACount = 95
  val minDVACount = 2
  val seed = 13
  new SimpleAllPairSampler(rolesetDir,outputDir,trainTimeEnd,sampleCount,seed).runSampling

}
