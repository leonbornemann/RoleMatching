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
  val compatibilityGroupDataDir = if(args.size==5) Some(new File(args(4)).listFiles().toIndexedSeq) else None
  val useCompatibilityBlockedData = compatibilityGroupDataDir.isDefined
  val seed = 13
  val simpleBlockingSampler = new SimpleBlockingSampler(rolesetDir,outputDir,trainTimeEnd,13,compatibilityGroupDataDir)
  simpleBlockingSampler.runSampling()
}
