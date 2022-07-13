package de.hpi.oneshot

import de.hpi.role_matching.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate

object DecayAndRelaxedMatchResultSetSizeMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val originalSampleDir = new File(args(0))
  val rolesetDir = new File(args(1))
  val outputDir = new File(args(2))
  val trainTimeEnd = LocalDate.parse("2016-05-07")
  val computer = new DecayAndRelaxedMatchResultSetSizeComputer(originalSampleDir,outputDir,rolesetDir,trainTimeEnd)
  computer.recomputeForOriginalSample()

}
