package de.hpi.role_matching.blocking.cbrb.role_tree.normal

import de.hpi.role_matching.blocking.cbrb.role_tree.AbstractRoleGroup
import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}

import scala.util.Random

case class NormalRoleGroup(roleIds: IndexedSeq[String]) extends AbstractRoleGroup with JsonWritable[NormalRoleGroup] {

  override def getIDPair(random: Random): (String, String) = {
    assert(roleIds.size>1)
    if(roleIds.size==2) (roleIds(0),roleIds(1))
    else {
      val i = random.nextInt(roleIds.size)
      var j = i
      assert(roleIds.size>=2)
      while(i==j)
        j = random.nextInt(roleIds.size) //reroll until it is not the same element
      (roleIds(i),roleIds(j))
    }
  }

  override def hasCandidates: Boolean = roleIds.size>1
}
object NormalRoleGroup extends JsonReadable[NormalRoleGroup]
