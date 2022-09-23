package de.hpi.role_matching.scalability

import de.hpi.util.GLOBAL_CONFIG

object RolesetGeneratorMain extends App {

  val n = args(0).toInt
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val avgDensity = 0.54
  val dv = 5.47
  val dvSTD = 6.82
}
