package de.hpi.oneshot

import de.hpi.oneshot.SinglePairCompatibilityCheck.{id1, id2}
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.{RoleLineage, Roleset}

import java.time.LocalDate

object SinglePairCompatibilityCheck extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val rs = Roleset.fromJsonFile("/home/leon/data/dataset_versioning/finalExperiments/nodecayrolesets_wikipedia/education.json")
  //infobox military conflict||248802||33644901-0||combatant1_🔗_extractedLink0_infobox military conflict||50236||32543590-0||combatant1_🔗_extractedLink0
  val id1 = "infobox book||19433458||240357062-0||genre"
  val id2 = "infobox book||19869298||246644690-0||genre"
  val trainTimeEnd = LocalDate.parse("2016-05-07")
  private val map = rs.getStringToLineageMap
  println(map.keySet.filter(_.contains("19433458")))
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
