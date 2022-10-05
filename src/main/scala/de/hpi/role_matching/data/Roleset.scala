package de.hpi.role_matching.data

import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}

import java.time.LocalDate
import scala.collection.immutable.IndexedSeq
import scala.util.Random

/***
 * Representation of a set of roles
 *
 * @param rolesSortedByID
 * @param positionToRoleLineage
 */
case class Roleset(rolesSortedByID: IndexedSeq[String], positionToRoleLineage:Map[Int,RoleLineageWithID]) extends JsonWritable[Roleset]{

  def valueAppearanceInLineageDistribution = {
    ValueDistribution(posToRoleLineage
      .values
      .toIndexedSeq
      .flatMap(_.lineage.values.filter(v => !RoleLineage.isWildcard(v)))
      .map(v => (v,1))
      .groupBy(_._1)
      .map{case (v,arr) => (v,arr.size)})
  }

  def exportWithProlongedTime(factor:Int) = {
    val result = Roleset(rolesSortedByID,positionToRoleLineage
      .map(t => {
        val newRl = t._2.roleLineage.toRoleLineage.toNewTimeScale(factor)
        (t._1,newRl.toIdentifiedRoleLineage(t._2.id))
      }))
    result
  }


  def subsample(downsamplingRatio: Double,random:Random) = {
    val map = getStringToLineageMap
    val rolesSortedNew = random
      .shuffle(rolesSortedByID)
      .take((downsamplingRatio*rolesSortedByID.size).toInt)
    val posToRoleLineageNew = rolesSortedNew
      .zipWithIndex
      .map{case (s,i) => (i,map(s))}
      .toMap
    Roleset(rolesSortedNew,posToRoleLineageNew)
  }

  def toNonCaseClass(trainTimeEnd:LocalDate): NonCaseClassRoleset = new NonCaseClassRoleset(this,trainTimeEnd)

  def wildcardValues = RoleLineage.WILDCARD_VALUES

  def getStringToLineageMap = {
    positionToRoleLineage.map{case (i,l) => (rolesSortedByID(i),l)}
  }

  rolesSortedByID.zipWithIndex.foreach(t => assert(positionToRoleLineage(t._2).id==t._1))

  val posToRoleLineage = positionToRoleLineage.map(t => (t._1,t._2.roleLineage.toRoleLineage))
}
object Roleset extends JsonReadable[Roleset]{

  def fromRoles(roles:IndexedSeq[RoleLineageWithID]) = {
    val posToLineage = roles
      .sortBy(_.id)
      .zipWithIndex
      .map(t => (t._2,t._1))
      .toMap
    val roleIdsSorted = roles
      .map(_.id)
      .sorted
    Roleset(roleIdsSorted,posToLineage)
  }

  def fromJsonFile(path: String,syntheticMissingDataShare:Option[Double],trainTimeEnd:Option[LocalDate],random:Option[Random])(implicit m: Manifest[Roleset]): Roleset = {
    if(syntheticMissingDataShare.isEmpty) {
      super.fromJsonFile(path)
    } else {
      val rs = super.fromJsonFile(path)
      val rolesNew = rs.positionToRoleLineage
        .map{ case (id,rl) => (id,rl.roleLineage.toRoleLineage.addSyntheticallyMissingData(syntheticMissingDataShare.get,trainTimeEnd.get,random.get).toIdentifiedRoleLineage(rl.id))}
      Roleset(rs.rolesSortedByID,rolesNew)
    }
  }

}
