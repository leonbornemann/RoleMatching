package de.hpi.role_matching.evaluation

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.text.DecimalFormat
import java.time.temporal.ChronoUnit

object RolesetStatisticsPrintMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputDir = new File(args(0))
  val statisticsPrinter = new RolesetStatisticsPrinter()
  statisticsPrinter.printSchema()

  statisticsPrinter.dsNameToAbbrev
    .keySet
    .toIndexedSeq
    .sorted
    .foreach(dsName => {
      val rs = Roleset.fromJsonFile(inputDir.getAbsolutePath + s"/$dsName.json")
      statisticsPrinter.printStats(rs,dsName)

    })


}
