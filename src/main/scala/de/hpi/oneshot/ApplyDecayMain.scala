package de.hpi.oneshot

import de.hpi.role_matching.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate

object ApplyDecayMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val rolesetDir = new File(args(1))
  val trainTimeEnd = LocalDate.parse(args(2))
  val resultDir = args(3)
  new File(resultDir).mkdir()
  val decayThreshold = 0.7
  rolesetDir.listFiles().foreach(f => {
    println(s"Processing ${f}")
    val resultFile = resultDir + "/" + f.getName
    new DecayApplyer(f,decayThreshold,trainTimeEnd,resultFile)
      .applyDecay()
  })

}
