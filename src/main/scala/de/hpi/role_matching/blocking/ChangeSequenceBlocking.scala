package de.hpi.role_matching.blocking

import de.hpi.role_matching.cbrm.data.{RoleLineage, Roleset}

import java.time.LocalDate

class ChangeSequenceBlocking(roleset: Roleset, trainTimeEnd: LocalDate) extends SimpleGroupBlocker{
  override val groups: Map[Any, Iterable[RoleLineage]] = roleset
    .posToRoleLineage
    .values
    .groupBy(rl => rl.nonWildcardValueSequenceBefore(trainTimeEnd))
}
