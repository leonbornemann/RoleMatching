package de.hpi.oneshot

import de.hpi.oneshot.SinglePairCompatibilityCheck.{id1, id2}
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.{RoleLineage, Roleset}

import java.time.LocalDate

object SinglePairCompatibilityCheck extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val rs = Roleset.fromJsonFile("/home/leon/data/dataset_versioning/finalExperiments/rolesets/military.json")
  val id1 = "infobox military conflict||10039822||114916240-0||combatant2"
  val id2 = "infobox military conflict||23278974||299908088-0||combatant1_\uD83D\uDD17_extractedLink1"
  val trainTimeEnd = LocalDate.parse("2016-05-07")
  private val map = rs.getStringToLineageMap
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
