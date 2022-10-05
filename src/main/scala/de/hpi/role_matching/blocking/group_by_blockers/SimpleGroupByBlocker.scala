package de.hpi.role_matching.blocking.group_by_blockers

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.data.{RoleLineage, RoleLineageWithID, RoleMatchCandidateIds, RoleReference, Roleset, ValueTransition}
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate

abstract class SimpleGroupByBlocker(roleset: Roleset, trainTimeEnd: LocalDate) extends StrictLogging{

  def simulatePerfectParallelCandidateSerialization(resultDir: File, getName: String, i: Int, nThreads: Int) = {
    val references = RoleLineageWithID.toReferences(roleset.positionToRoleLineage.values.toIndexedSeq.sortBy(_.id), trainTimeEnd)
    val tupleToNonWcTransitions = references
      .map(t => {
        (t,t.getRole.informativeValueTransitions)
      })
      .toMap
    val matchIt = matchIterator(references)
    var processed = 0
    val total = getMatchCount()
    val pr = new PrintWriter(resultDir.getAbsolutePath + "/candidates.json")
    val curTime = System.currentTimeMillis()
    while (processed < 1000000 && matchIt.hasNext){
      val (e1,e2) = matchIt.next()
      val idEdge = RoleMatchCandidateIds(e1.getRoleID,e2.getRoleID)
      if(tupleToNonWcTransitions(e1).exists(t => tupleToNonWcTransitions(e2).contains(t))){
        idEdge.appendToWriter(pr,false,true)
      }
      processed +=1
    }
    pr.close()
    val timeAfter = System.currentTimeMillis()
    val totalTimeSeconds = (timeAfter-curTime) / 1000.0
    if(processed<total){
      logger.debug(s"Took $totalTimeSeconds for ${processed / total.toDouble} (single threaded), total is : $total")
      logger.debug(s"Estimated additional time needed [s]: ${(totalTimeSeconds * (1 - processed / total.toDouble)) / nThreads}")
    } else {
      logger.debug(s"Took $totalTimeSeconds for ${processed} (single thread)")
    }
  }

  def getGroup(getRole: RoleLineage): Any

  def groupsWithReference(references:IndexedSeq[RoleReference]): Map[Any, Iterable[RoleReference]] = {
    references
      .groupBy(rl => getGroup(rl.getRole))
  }

  val groups: Map[Any, Iterable[RoleLineage]] = roleset
    .posToRoleLineage
    .values
    .groupBy(rl => getGroup(rl))

  def getPairwiseMatches(input: Iterable[RoleReference]) = {
    val asIndexed = input.toIndexedSeq
    val matchResult = collection.mutable.ArrayBuffer[(RoleReference,RoleReference)]()
    for( i <- 0 until asIndexed.size){
      for(j <- i+1 until asIndexed.size){
        val ref1 = asIndexed(i)
        val ref2 = asIndexed(j)
        matchResult.append((ref1,ref2))
      }
    }
    matchResult.toIndexedSeq
  }

  def matchIterator(references:IndexedSeq[RoleReference]) = {
    groupsWithReference(references)
      .iterator
      .flatMap{case (key,matches) => getPairwiseMatches(matches)}
  }

  def gaussSum(size: Int) = size.toLong * (size.toLong + 1) / 2

  def getMatchCount() = {
    groups.map(g => gaussSum(g._2.size - 1)).sum
  }

}
