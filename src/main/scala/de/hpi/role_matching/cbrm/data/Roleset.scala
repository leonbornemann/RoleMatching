package de.hpi.role_matching.cbrm.data

import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}

/***
 * Representation of a set of roles
 * @param rolesSortedByID
 * @param positionToRoleLineage
 */
case class Roleset(rolesSortedByID: IndexedSeq[String], positionToRoleLineage:Map[Int,RoleLineageWithID]) extends JsonWritable[Roleset]{

  def getStringToLineageMap = {
    positionToRoleLineage.map{case (i,l) => (rolesSortedByID(i),l)}
  }

  rolesSortedByID.zipWithIndex.foreach(t => assert(positionToRoleLineage(t._2).id==t._1))

  val posToFactLineage = positionToRoleLineage.map(t => (t._1,t._2.factLineage.toFactLineage))
}
object Roleset extends JsonReadable[Roleset]
