package de.hpi.role_matching.cbrm.data

import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable, LocalDateKeySerializer, LocalDateSerializer}
import org.json4s.DefaultFormats

/***
 * Representation of a set of roles
 *
 * @param rolesSortedByID
 * @param positionToRoleLineage
 */
case class Roleset(rolesSortedByID: IndexedSeq[String], positionToRoleLineage:Map[Int,RoleLineageWithID]) extends JsonWritable[Roleset]{
  def wildcardValues = RoleLineage.WILDCARD_VALUES

  def getStringToLineageMap = {
    positionToRoleLineage.map{case (i,l) => (rolesSortedByID(i),l)}
  }

  rolesSortedByID.zipWithIndex.foreach(t => assert(positionToRoleLineage(t._2).id==t._1))

  val posToRoleLineage = positionToRoleLineage.map(t => (t._1,t._2.roleLineage.toRoleLineage))
}
object Roleset extends JsonReadable[Roleset]{

}
