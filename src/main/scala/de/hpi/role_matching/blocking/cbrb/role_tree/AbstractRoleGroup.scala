package de.hpi.role_matching.blocking.cbrb.role_tree

import de.hpi.role_matching.blocking.cbrb.role_tree.bipartite.BipartiteRoleGroup
import de.hpi.role_matching.blocking.cbrb.role_tree.normal.NormalRoleGroup
import de.hpi.role_matching.data.{RoleLineageWithID, RoleMatchCandidateIds}
import de.hpi.role_matching.evaluation.blocking.ground_truth.BasicStatRow
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate
import scala.io.Source
import scala.util.Random

abstract class AbstractRoleGroup {

  def getIDPair(random: Random):(String,String)

  def hasCandidates:Boolean

  def tryDrawSample(random:Random, roleMap:Map[String, RoleLineageWithID], trainTimeEnd: LocalDate):Option[RoleMatchCandidateIds] = {
    val (idLeft,idRight) = getIDPair(random)
    val left = roleMap(idLeft).roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, trainTimeEnd)
    val right = roleMap(idRight).roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, trainTimeEnd)
    val evidence = left.allNonWildcardTimestamps.toSet.intersect(right.allNonWildcardTimestamps.toSet).size
    if (left.getOverlapEvidenceCount(right) > 1 && evidence > 1) {
      val statRow = new BasicStatRow(left, right, trainTimeEnd)
      assert(statRow.remainsValidFullTimeSpan)
      assert(idRight!=idLeft)
      if(idRight<idLeft)
        Some(RoleMatchCandidateIds(idRight, idRight))
      else
        Some(RoleMatchCandidateIds(idLeft,idRight))
    } else {
      None
    }
  }
}
object AbstractRoleGroup{

  def parseRoleCompatibilityGroupsFromFile(f: File):IndexedSeq[AbstractRoleGroup] = {
    Source
      .fromFile(f)
      .getLines()
      .toIndexedSeq
      .map(s => {
        try {
          val res = NormalRoleGroup.fromJsonString(s)
          res
        } catch {
          case (e:Throwable) => BipartiteRoleGroup.fromJsonString(s)
          }
      })
      .filter(_.hasCandidates)
  }

}
