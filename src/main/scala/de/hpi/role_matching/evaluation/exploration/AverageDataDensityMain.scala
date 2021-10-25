package de.hpi.role_matching.evaluation.exploration

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.compatibility.graph.representation.slim.VertexLookupMap

import java.time.Period
import java.time.temporal.ChronoUnit

object AverageDataDensityMain extends App {

  val vertexLookupDir = "/home/leon/data/dataset_versioning/vertexLookupMaps"
  val dsNames = Seq("politics","military","education","football","tv_and_film")
  GLOBAL_CONFIG.setDatesForDataSource("wikipedia")
  val totalDurationDays = ChronoUnit.DAYS.between(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,GLOBAL_CONFIG.STANDARD_TIME_FRAME_END)
  val finalRes = dsNames.flatMap(dsName => {
    val map = VertexLookupMap.fromJsonFile(vertexLookupDir + s"/$dsName.json")
    val densities = map.posToFactLineage.values
      .map(fl => {
        val nonWildcardDuration = fl.nonWildcardDuration(GLOBAL_CONFIG.STANDARD_TIME_FRAME_END)
        nonWildcardDuration / totalDurationDays.toDouble
      })
    val sumOfDensity = densities.sum
    val res = sumOfDensity / map.posToFactLineage.size
    println(s"$dsName: $res")
    densities
  })
  println(s"Total: ${finalRes.sum / finalRes.size}")
}
