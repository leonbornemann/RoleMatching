package de.hpi.role_matching.blocking.group_by_blockers

import de.hpi.role_matching.data.{RoleLineage, Roleset}

import java.time.LocalDate

class CQMBlocking(roleset: Roleset, trainTimeEnd: LocalDate) extends SimpleGroupByBlocker(roleset,trainTimeEnd) {


  override def getGroup(rl: RoleLineage): Any = rl.valueSequenceBefore(trainTimeEnd)
}
