package de.hpi.role_matching.evaluation.blocking.missing_values

import de.hpi.role_matching.data.{ReservedChangeValues, RoleLineage, Roleset}
import de.hpi.util.GLOBAL_CONFIG

import java.time.LocalDate
import scala.util.Random

class MissingDataTransformer(rs: Roleset, random: Random, trainTimeEnd: LocalDate, rolesInDGS:Set[String], rolesInRGS:Set[String]) {

  val rolesInDGSShuffled = random.shuffle(rolesInDGS
    .toIndexedSeq
    .sorted
  )

  val rolesInRGSShuffled = random.shuffle(rolesInRGS
    .toIndexedSeq
    .sorted
  )

  val stringToLineageMap = rs.getStringToLineageMap

  def getChanges(ids: Set[String]) = {
    random.shuffle(stringToLineageMap
      .filter{case (k,l) => ids.contains(k)}
      .map(t => (t._1,t._2.roleLineage.toRoleLineage))
      .toIndexedSeq
      .flatMap{case (s,lineage) => lineage
        .lineage
        .filter(t => !RoleLineage.isWildcard(t._2) && t._1.isBefore(trainTimeEnd))
        .map(t => (s,t._1))
        .toIndexedSeq}
    )
  }

  val changesWithoutWildcardsShuffled = getChanges(stringToLineageMap.keySet.diff(rolesInDGS).diff(rolesInRGS))
  val changesInRGSShuffled = getChanges(rolesInRGS)
  val changesInDGSShuffled = getChanges(rolesInDGS)


  def removePercentageOfChangesAndReplaceWithWildcardLeaveAtLeastOneTransition(percentageToDelete:Double,
                                                      changesWithoutWildcardsShuffled: IndexedSeq[(String, LocalDate)],
                                                      strToLineage: Map[String, RoleLineage]) = {
    val targetCount = (percentageToDelete*changesWithoutWildcardsShuffled.size).toInt
    val it = changesWithoutWildcardsShuffled.iterator
    var curCount = 0
    while(curCount < targetCount && it.hasNext){
      val (s,k) = it.next()
      val res = strToLineage(s).lineage.remove(k)
      if(!res.isDefined)
        println("Was already removed")
      else {
        strToLineage(s).lineage(k) = ReservedChangeValues.DECAYED
        if(strToLineage(s).getValueTransitionSet(true,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd)).size==0){
          //revert the change
          strToLineage(s).lineage(k) = res.get
        } else {
          strToLineage(s).removeSubsequentDuplicates()
          curCount+=1
        }
      }
    }
    if(curCount < targetCount){
      println(s"Did not achieve target: $curCount / $targetCount")
    }
  }

  def removePercentageOfChangesAndReplaceWithWildcard(percentageToDelete:Double,
                                                      changesWithoutWildcardsShuffled: IndexedSeq[(String, LocalDate)],
                                                      strToLineage: Map[String, RoleLineage]) = {
    var numEmptyTransitions = 0
    changesWithoutWildcardsShuffled
      .take((percentageToDelete*changesWithoutWildcardsShuffled.size).toInt)
      .foreach{case (s,k) => {
        val res = strToLineage(s).lineage.remove(k)
        if(!res.isDefined)
          println("Was already removed")
        else {
          strToLineage(s).lineage(k) = ReservedChangeValues.DECAYED
          if(strToLineage(s).getValueTransitionSet(true,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd)).size==0){
            numEmptyTransitions+=1
          }
          strToLineage(s).removeSubsequentDuplicates()
        }
      }}
    numEmptyTransitions
  }

  def getTransformedRoleFile(percentageToDelete:Double):Roleset = {
    val strToLineage = rs.getStringToLineageMap
      .map(t => ((t._1,t._2.roleLineage.toRoleLineage)))
    val numEmpty1 = removePercentageOfChangesAndReplaceWithWildcard(percentageToDelete,changesWithoutWildcardsShuffled,strToLineage)
    val numEmpty2 = removePercentageOfChangesAndReplaceWithWildcard(percentageToDelete,changesInRGSShuffled,strToLineage)
    val numEmpty3 = removePercentageOfChangesAndReplaceWithWildcard(percentageToDelete,changesInDGSShuffled,strToLineage)
    println(numEmpty1,numEmpty2,numEmpty3,numEmpty1+numEmpty2+numEmpty3)
    val withIndex = strToLineage
      .toIndexedSeq
      .sortBy(_._1)
      .zipWithIndex
    val posToLineage = withIndex
      .map{case ((s,l),i) => (i,l.toIdentifiedRoleLineage(s))}
    val keysSorted = withIndex
      .map(_._1._1)
    Roleset(keysSorted,posToLineage.toMap)

  }
}
