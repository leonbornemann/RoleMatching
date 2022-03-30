package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID
import de.hpi.role_matching.cbrm.data.RoleLineageWithID
import de.hpi.role_matching.evaluation.tuning.BasicStatRow
import de.hpi.role_matching.playground.{JsonReadable, JsonWritable}

import java.time.LocalDate
import scala.util.Random

case class BipartiteRoleGroup(leftIds: IndexedSeq[String], rightIds: IndexedSeq[String]) extends RoleGroup with JsonWritable[BipartiteRoleGroup] {
  override def getIDPair(random: Random): (String, String) = {
    assert(leftIds.size>0 && rightIds.size>0)
    val idLEft = leftIds(random.nextInt(leftIds.size))
    val idRight = leftIds(random.nextInt(rightIds.size))
    (idLEft,idRight)
  }
}
object BipartiteRoleGroup extends JsonReadable[BipartiteRoleGroup]
