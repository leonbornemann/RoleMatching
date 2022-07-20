package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.role_matching.playground.{JsonReadable, JsonWritable}

import scala.util.Random

case class BipartiteRoleGroup(leftIds: IndexedSeq[String], rightIds: IndexedSeq[String]) extends RoleGroup with JsonWritable[BipartiteRoleGroup] {
  override def getIDPair(random: Random): (String, String) = {
    assert(leftIds.size>0 && rightIds.size>0)
    val idLEft = leftIds(random.nextInt(leftIds.size))
    val idRight = leftIds(random.nextInt(rightIds.size))
    (idLEft,idRight)
  }

  override def hasCandidates: Boolean = leftIds.size>=1 && rightIds.size>=1
}
object BipartiteRoleGroup extends JsonReadable[BipartiteRoleGroup]
