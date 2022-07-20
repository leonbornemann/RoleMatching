package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate

object SimpleBlockingSamplerMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val rolesetDir = new File(args(1))
  val outputDir = args(2)
  val trainTimeEnd = LocalDate.parse(args(3))
  val sampleGroundTruth = args(4).toBoolean
  val sampleCount = if(args.size == 6) Some(args(5).toInt) else None
  val minVACount = 95
  val minDVACount = 2
  val seed = 13
  val simpleBlockingSampler = new SimpleBlockingSampler(rolesetDir,outputDir,trainTimeEnd,13,minVACount,minDVACount,sampleGroundTruth,sampleCount)
  simpleBlockingSampler.runSampling()
}
