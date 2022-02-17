package de.hpi.oneshot

import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.{RoleLineage, Roleset}

import java.io.File

object RolesetExploration extends App {
  val rolesets = new File(args(0)).listFiles().map(f => (f.getName,Roleset.fromJsonFile(f.getAbsolutePath).getStringToLineageMap)).toMap
  println(s"sizes: ${rolesets.map(_._2.size)}")
  val filtered = rolesets.head._2.filter(_._2.roleLineage.lineage.values.filter(v => !RoleLineage.isWildcard(v)).size>2).keySet
  println(rolesets.map(_._1))
  filtered.foreach(k => {
    val lineages = rolesets.map(t => t._2(k)).toIndexedSeq
    val e1 = SimpleCompatbilityGraphEdge(lineages(0),lineages(1))
    val e2 = SimpleCompatbilityGraphEdge(lineages(1),lineages(2))
    e1.printTabularEventLineageString
    e2.printTabularEventLineageString
    println()
  })

}
