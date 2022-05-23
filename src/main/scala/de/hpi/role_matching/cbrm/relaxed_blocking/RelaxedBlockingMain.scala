package de.hpi.role_matching.cbrm.relaxed_blocking

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate

object RelaxedBlockingMain extends App {
  val datasource = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(datasource)
  private val rolesetFile = args(1)
  val trainTimeEnd = LocalDate.parse(args(2))
  val targetPercentage = args(3).toDouble
  val resultDir = new File(args(4))
  val roleset = Roleset.fromJsonFile(rolesetFile)
  //  val roleSamplingRate = args(5).toDouble
  //  val timestampSamplingRate = args(6).toDouble
  val relaxedBlocker = new RelaxedBlocker(roleset, trainTimeEnd, resultDir, targetPercentage)

  relaxedBlocker.executeBlocking()

}
