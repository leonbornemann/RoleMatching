package de.hpi.role_matching.blocking

import de.hpi.role_matching.cbrm.data.{RoleLineage, Roleset}

import java.time.LocalDate

class VSMBlocking(roleset: Roleset, trainTimeEnd: LocalDate) extends SimpleGroupByBlocker {
  override val groups: Map[Any, Iterable[RoleLineage]] = roleset
    .posToRoleLineage
    .values
    .groupBy(rl => rl.nonWildcardValueSetBefore(trainTimeEnd))
}
