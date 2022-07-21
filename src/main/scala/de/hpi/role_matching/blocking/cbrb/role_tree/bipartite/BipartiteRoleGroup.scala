package de.hpi.role_matching.blocking.cbrb.role_tree.bipartite

import de.hpi.role_matching.blocking.cbrb.role_tree.AbstractRoleGroup
import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}

import scala.util.Random

case class BipartiteRoleGroup(leftIds: IndexedSeq[String], rightIds: IndexedSeq[String]) extends AbstractRoleGroup with JsonWritable[BipartiteRoleGroup] {
  override def getIDPair(random: Random): (String, String) = {
    assert(leftIds.size>0 && rightIds.size>0)
    val idLEft = leftIds(random.nextInt(leftIds.size))
    val idRight = leftIds(random.nextInt(rightIds.size))
    (idLEft,idRight)
  }

  override def hasCandidates: Boolean = leftIds.size>=1 && rightIds.size>=1
}
object BipartiteRoleGroup extends JsonReadable[BipartiteRoleGroup]
