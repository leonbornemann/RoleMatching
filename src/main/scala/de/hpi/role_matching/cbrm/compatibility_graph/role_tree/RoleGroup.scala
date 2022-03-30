package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID
import de.hpi.role_matching.cbrm.data.RoleLineageWithID
import de.hpi.role_matching.evaluation.tuning.BasicStatRow

import java.io.File
import java.time.LocalDate
import scala.io.Source
import scala.util.Random

abstract class RoleGroup {

  def getIDPair(random: Random):(String,String)

  def hasCandidates:Boolean

  def tryDrawSample(random:Random, roleMap:Map[String, RoleLineageWithID], trainTimeEnd: LocalDate):Option[SimpleCompatbilityGraphEdgeID] = {
    val (idLeft,idRight) = getIDPair(random)
    val left = roleMap(idLeft).roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, trainTimeEnd)
    val right = roleMap(idRight).roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, trainTimeEnd)
    val evidence = left.allNonWildcardTimestamps.toSet.intersect(right.allNonWildcardTimestamps.toSet).size
    if (left.getOverlapEvidenceCount(right) > 1 && evidence > 1) {
      val statRow = new BasicStatRow(left, right, trainTimeEnd)
      assert(statRow.remainsValidFullTimeSpan)
      assert(idRight!=idLeft)
      if(idRight<idLeft)
        Some(SimpleCompatbilityGraphEdgeID(idRight, idRight))
      else
        Some(SimpleCompatbilityGraphEdgeID(idLeft,idRight))
    } else {
      None
    }
  }
}
object RoleGroup{

  def parseRoleCompatibilityGroupsFromFile(f: File):IndexedSeq[RoleGroup] = {
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
