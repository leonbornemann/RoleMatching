package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate

object SimpleBlockingSamplerMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val rolesetDir = new File(args(1))
  val outputDir = args(2)
  val trainTimeEnd = LocalDate.parse(args(3))
  val minVACount = 95
  val minDVACount = 2
  val seed = 13
  val simpleBlockingSampler = new SimpleBlockingSampler(rolesetDir,outputDir,trainTimeEnd,13,minVACount,minDVACount)
  simpleBlockingSampler.runSampling()
}
