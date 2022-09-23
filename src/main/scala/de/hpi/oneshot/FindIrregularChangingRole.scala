package de.hpi.oneshot

import de.hpi.role_matching.data.{RoleLineage, RoleLineageWithID, RoleMatchCandidate, Roleset}
import de.hpi.util.GLOBAL_CONFIG

import java.time.temporal.ChronoUnit


object FindIrregularChangingRole extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val rs = Roleset.fromJsonFile("/home/leon/data/dataset_versioning/finalExperiments/nodecayrolesets_wikipedia/football.json")

  def getTimeToChange(t: RoleLineage) = {
    val withIndex = t
      .lineage
      .tail
      .keySet
      .toIndexedSeq
      .zipWithIndex
    withIndex
      .tail
      .map{case (ld,i) => ChronoUnit.DAYS.between(withIndex(i-1)._1,ld).toInt}
  }

  val res = rs.getStringToLineageMap
    .values
    .map(t => (t.id,t.roleLineage.toRoleLineage))
    .filter{case (id,t) => t.lineage.size > 20 && t.nonWildCardValues.size>10}
    .filter{case (id,t) => {
      val avg = getTimeToChange(t).sum / getTimeToChange(t).size.toDouble
      getSTD(getTimeToChange(t),avg) > 2*avg
    }}
    .filter(_._1.contains("goals"))
  println(res.size)
  for (elem <- res.take(100)) {
    println(elem._1)
    RoleMatchCandidate(elem._2.toIdentifiedRoleLineage(elem._1),elem._2.toIdentifiedRoleLineage(elem._1))
      .printTabularEventLineageString
    println(elem._2.lineage.keySet.toIndexedSeq)
    val values = elem._2.lineage.keySet.toIndexedSeq.tail.map(_.toEpochDay - GLOBAL_CONFIG.STANDARD_TIME_FRAME_START.toEpochDay)
    println(values)
    println(values.map(l => l + " 1").mkString(" "))
  }
  private def getSTD(dvs: Iterable[Int], avg: Double) = {
    Math.sqrt(dvs.map(dv => Math.pow((avg - dv).abs, 2)).sum / dvs.size.toDouble)
  }

}