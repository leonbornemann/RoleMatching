package de.hpi.role_matching.evaluation.statistics

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.text.DecimalFormat
import java.time.temporal.ChronoUnit

object RolesetStatisticsPrintMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputDir = new File(args(0))
  val dsNameToAbbrev = Map(
    ("education", "ED"),
    ("politics", "PO"),
    ("military", "MI"),
    ("tv_and_film", "TV"),
    ("football", "FO")
  )
  println("Dataset,#roles,#DV,avgDensity")
  val formatter = new DecimalFormat("#.##")
  dsNameToAbbrev
    .keySet
    .toIndexedSeq
    .sorted
    .foreach(dsName => {
      val rs = Roleset.fromJsonFile(inputDir.getAbsolutePath + s"/$dsName.json")
      val size = {
        rs.rolesSortedByID.size
      }
      val dvs = rs.posToRoleLineage
        .values
        .map(rl => rl.nonWildcardValueSetBefore(GLOBAL_CONFIG.STANDARD_TIME_FRAME_END.plusDays(1)).size)
      val avgDVCount = dvs
        .sum / size.toDouble
      //std:
      val stdDV = getSTD(dvs, avgDVCount)
      val formattedAVGDVCount = formatter.format(avgDVCount)
      val densities = rs.posToRoleLineage
        .values
        .map(rl => rl.nonWildcardDuration(GLOBAL_CONFIG.STANDARD_TIME_FRAME_END.plusDays(1)) / ChronoUnit.DAYS.between(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, GLOBAL_CONFIG.STANDARD_TIME_FRAME_END).toDouble)
      val avgDensity = densities
        .sum / rs.posToRoleLineage.size.toDouble
      val stdDensity = getSTDDouble(densities, avgDensity)
      val formattedAvgDensity = formatter.format(avgDensity)
      println(s"${dsNameToAbbrev(dsName)},$size,$formattedAVGDVCount (${formatter.format(stdDV)}),$formattedAvgDensity (${formatter.format(stdDensity)})")
    })

  private def getSTD(dvs: Iterable[Int], avg: Double) = {
    Math.sqrt(dvs.map(dv => Math.pow((avg - dv).abs, 2)).sum / dvs.size.toDouble)
  }

  private def getSTDDouble(dvs: Iterable[Double], avg: Double) = {
    Math.sqrt(dvs.map(dv => Math.pow((avg - dv).abs, 2)).sum / dvs.size.toDouble)
  }
}
