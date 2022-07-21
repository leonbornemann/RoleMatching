package de.hpi.role_matching.evaluation.blocking.sampling

import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate

object TVA_DVA_SamplerMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val rolesetDir = new File(args(1))
  val outputDir = args(2)
  val trainTimeEnd = LocalDate.parse(args(3))
  val sampleGroundTruth = args(4).toBoolean
  val seed = args(5).toLong
  val sampleCount = if (args.size == 7) Some(args(6).toInt) else None
  val minVACount = 95
  val minDVACount = 2
  val simpleBlockingSampler = new SimpleBlockingSampler(rolesetDir, outputDir, trainTimeEnd, seed, minVACount, minDVACount, sampleGroundTruth, sampleCount)
  simpleBlockingSampler.runSampling()
}
