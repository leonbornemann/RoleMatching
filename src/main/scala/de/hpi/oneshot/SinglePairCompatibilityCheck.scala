package de.hpi.oneshot

import de.hpi.oneshot.SinglePairCompatibilityCheck.{id1, id2}
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.{RoleLineage, Roleset}

import java.time.LocalDate

object SinglePairCompatibilityCheck extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val rs = Roleset.fromJsonFile("/home/leon/data/dataset_versioning/finalExperiments/rolesets/military.json")
  val id1 = "infobox weapon||11446814||178219904-0||origin"
  val id2 = "infobox weapon||9774757||374746808-0||origin"
  val trainTimeEnd = LocalDate.parse("2016-05-07")
  private val map = rs.getStringToLineageMap
  println(map.keySet.filter(_.contains("|11446814||178219904-0||origin")))
    println(map.keySet.filter(_.contains("|9774757||374746808-0||origin")))
  //println(map.get("infobox weapon||9774757||374746808-0||origin"))
  printForIds(id1, id2)
  println()

  def printForIds(id1:String,id2:String) = {
    val rl1 = map(id1).roleLineage.toRoleLineage
    val rl2 = map(id2).roleLineage.toRoleLineage
    val compatibility = rl1.getCompatibilityTimePercentage(rl2,trainTimeEnd)
    println(SimpleCompatbilityGraphEdge(map(id1),map(id2)).printTabularEventLineageString)
    println(compatibility)
  }
}
