package de.hpi.role_matching.blocking

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.{RoleLineage, Roleset, ValueTransition}

import java.time.LocalDate

class TSMBlocking(roleset: Roleset, trainTimeEnd: LocalDate) extends SimpleGroupByBlocker {

  override val groups: Map[Any, Iterable[RoleLineage]] = {
    roleset
      .posToRoleLineage
      .values
      .groupBy(rl => rl.getValueTransitionSet(true,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd)))
  }

  def idGroups:Map[Set[ValueTransition], IndexedSeq[String]] ={
    roleset
      .getStringToLineageMap
      .groupBy{case (k,rlWID) =>
        rlWID.roleLineage
          .toRoleLineage
          .getValueTransitionSet(true,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd))}
      .map(t => (t._1,t._2.keySet.toIndexedSeq.sorted))
  }
}
