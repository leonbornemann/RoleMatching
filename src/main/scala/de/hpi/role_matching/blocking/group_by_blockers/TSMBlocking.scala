package de.hpi.role_matching.blocking.group_by_blockers

import de.hpi.role_matching.data.{RoleLineage, RoleLineageWithID, RoleReference, Roleset, ValueTransition}
import de.hpi.util.GLOBAL_CONFIG

import java.time.LocalDate

class TSMBlocking(roleset: Roleset, trainTimeEnd: LocalDate) extends SimpleGroupByBlocker(roleset,trainTimeEnd) {

  def idGroups:Map[Set[ValueTransition], IndexedSeq[String]] ={
    roleset
      .getStringToLineageMap
      .groupBy{case (k,rlWID) =>
        rlWID.roleLineage
          .toRoleLineage
          .getValueTransitionSet(true,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd))}
      .map(t => (t._1,t._2.keySet.toIndexedSeq.sorted))
  }

  override def getGroup(rl: RoleLineage): Any = rl.getValueTransitionSet(true,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd))
}
