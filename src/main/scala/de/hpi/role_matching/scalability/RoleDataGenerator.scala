package de.hpi.role_matching.scalability

import de.hpi.role_matching.data.{ReservedChangeValues, RoleLineage, RoleLineageWithHashMap, RoleLineageWithID, Roleset, ValueDistribution}
import de.hpi.util.GLOBAL_CONFIG

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate}
import scala.util.Random

class RoleDataGenerator(rs: Roleset,
                        valueInLineageDistribution: ValueDistribution[Any],
                        random:Random) {

  val lineages = rs
    .getStringToLineageMap
    .toIndexedSeq

  def randomizeValues(oldLineage: RoleLineageWithID,newID:String) = {
    val oldValues = oldLineage
      .roleLineage
      .toRoleLineage
      .nonWildCardValues
      .toIndexedSeq
    val newValues = valueInLineageDistribution
      .drawWeightedRandomValues(oldValues.size,random)
      .toIndexedSeq
    val oldToNew = oldValues
      .zip(newValues)
      .toMap
    val newMap = oldLineage
      .roleLineage
      .lineage
      .map{case (ld,v) => (ld,oldToNew.getOrElse(v,v))}
    RoleLineageWithID(newID,RoleLineageWithHashMap(newMap))
  }

  def generateNew(n: Int) = {
    (0 until n ).map{ i =>
      val (oldID,oldLineage) = lineages(random.nextInt(lineages.size))
      val id = s"${oldID}_$i"
      randomizeValues(oldLineage,id)
    }
  }

  def generate(n:Int) = {
    if(n < lineages.size){
      random
        .shuffle(lineages)
        .take(n)
        .map(_._2)
    } else {
      lineages.map(_._2) ++ generateNew(n - lineages.size)
    }
  }


}
