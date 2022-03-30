package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.role_matching.playground.{JsonReadable, JsonWritable}

import scala.util.Random

case class NormalRoleGroup(roleIds: IndexedSeq[String]) extends RoleGroup with JsonWritable[NormalRoleGroup] {

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
}
object NormalRoleGroup extends JsonReadable[NormalRoleGroup]