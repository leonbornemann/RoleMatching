package de.hpi.role_matching.evaluation

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.text.DecimalFormat
import java.time.temporal.ChronoUnit

class RolesetStatisticsPrinter() {
  def printSchema() = println("Dataset,#roles,#pairs,#DV,avgDensity")


  val dsNameToAbbrev = Map(
    ("education", "ED"),
    ("politics", "PO"),
    ("military", "MI"),
    ("tv_and_film", "TV"),
    ("football", "FO")
  )
  val formatter = new DecimalFormat("#.##")

  private def getSTD(dvs: Iterable[Int], avg: Double) = {
    Math.sqrt(dvs.map(dv => Math.pow((avg - dv).abs, 2)).sum / dvs.size.toDouble)
  }

  private def getSTDDouble(dvs: Iterable[Double], avg: Double) = {
    Math.sqrt(dvs.map(dv => Math.pow((avg - dv).abs, 2)).sum / dvs.size.toDouble)
  }

  def gaussSum(n: Int) = n.toLong * (n + 1).toLong / 2

  def printStats(rs: Roleset,dsName:String) = {
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
      .map(rl => rl.dataDensity(GLOBAL_CONFIG.STANDARD_TIME_FRAME_END))
    val avgDensity = densities
      .sum / rs.posToRoleLineage.size.toDouble
    val stdDensity = getSTDDouble(densities, avgDensity)
    val formattedAvgDensity = formatter.format(avgDensity)
    val name = dsNameToAbbrev.get(dsName).getOrElse(dsName)
    println(s"$name,$size,${gaussSum(size)},$formattedAVGDVCount (${formatter.format(stdDV)}),$formattedAvgDensity (${formatter.format(stdDensity)})")
  }

}
