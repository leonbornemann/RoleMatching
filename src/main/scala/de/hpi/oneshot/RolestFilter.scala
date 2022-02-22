package de.hpi.oneshot

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate

object RolestFilter extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val rolesetDir = new File(args(1))
  val dsNames = args(2).split(";")
  val trainTimeEnd = LocalDate.parse(args(3))
  val resultDir = new File(args(4))
  resultDir.mkdirs()
  val files = dsNames.map(s => rolesetDir.getAbsolutePath + s"/$s.json")
  files.foreach{case f =>
    println(s"processing $f")
    val roleset = Roleset.fromJsonFile(f)
    val byID = roleset.getStringToLineageMap
    val rolesNew = roleset.rolesSortedByID.filter(id => byID(id).roleLineage.toRoleLineage.isOfInterest(trainTimeEnd))
    val roleMapNew = rolesNew.zipWithIndex.map{case (id,i) => (i,byID(id))}.toMap
    val resultFile = new File(resultDir.getAbsolutePath + s"/${new File(f).getName}")
    println(s"Retained ${rolesNew.size} out of ${roleset.rolesSortedByID.size} (${rolesNew.size / roleset.rolesSortedByID.size.toDouble})")
    Roleset(rolesNew,roleMapNew).toJsonFile(resultFile)
  }
}
