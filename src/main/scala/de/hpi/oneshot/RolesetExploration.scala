package de.hpi.oneshot

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.RoleLineage.isWildcard
import de.hpi.role_matching.cbrm.data.{RoleLineage, Roleset}

import java.io.File
import java.time.LocalDate

object RolesetExploration extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val rolesets = new File(args(0)).listFiles().map(f => (f.getName,Roleset.fromJsonFile(f.getAbsolutePath).getStringToLineageMap)).toMap
  println(s"sizes: ${rolesets.map(_._2.size)}")
  val trainTmeEnd = LocalDate.parse("2017-04-29")
  rolesets.foreach{case (config,roleset) => {
    val rsFiltered = roleset.filter{ t =>
      val rl =t._2.roleLineage.toRoleLineage
      val valueSetTrain = rl.lineage.range(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTmeEnd).map{ case(_,v) => v}.toSet
        .filter(v => !isWildcard(v))
      val valueSetTest = rl.lineage.rangeFrom(trainTmeEnd).map(_._2).toSet
        .filter(v => !isWildcard(v))
      valueSetTrain.size>1 && valueSetTest.size>1
    }
    println(s"$config ${rsFiltered.size} / ${roleset.size} (${rsFiltered.size / roleset.size.toDouble})")
  }}
  //how many
  val filtered = rolesets.head._2.filter(_._2.roleLineage.lineage.values.filter(v => !RoleLineage.isWildcard(v)).size>2).keySet
  println(rolesets.map(_._1))
//  filtered.foreach(k => {
//    val lineages = rolesets.map(t => t._2(k)).toIndexedSeq
//    val e1 = SimpleCompatbilityGraphEdge(lineages(0),lineages(1))
//    val e2 = SimpleCompatbilityGraphEdge(lineages(1),lineages(2))
//    e1.printTabularEventLineageString
//    e2.printTabularEventLineageString
//    println()
//  })

}
