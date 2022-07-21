package de.hpi.role_matching.blocking.group_by_blockers

import de.hpi.role_matching.data.{RoleLineage, Roleset}

import java.time.LocalDate

class QSMBlocking(roleset: Roleset, trainTimeEnd: LocalDate) extends SimpleGroupByBlocker{

  override val groups: Map[Any, Iterable[RoleLineage]] = roleset
    .posToRoleLineage
    .values
    .groupBy(rl => rl.valueSequenceBefore(trainTimeEnd))
}
