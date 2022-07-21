package de.hpi.role_matching.evaluation.statistics

import de.hpi.role_matching.data.Roleset

import java.io.File

object PrintNumPairsMain extends App {
  val rolesetDir = args(0)
  new File(rolesetDir)
    .listFiles()
    .map(f => (f, Roleset.fromJsonFile(f.getAbsolutePath)))
    .foreach { case (f, rs) =>
      println(f.getName.split("\\.")(0), gaussSum(rs.rolesSortedByID.size))
    }

  def gaussSum(n: Int) = n.toLong * (n + 1).toLong / 2

}
