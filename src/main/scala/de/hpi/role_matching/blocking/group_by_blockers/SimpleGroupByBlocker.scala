package de.hpi.role_matching.blocking.group_by_blockers

import de.hpi.role_matching.data.RoleLineage

trait SimpleGroupByBlocker {

  val groups: Map[Any, Iterable[RoleLineage]]

  def gaussSum(size: Int) = size.toLong * (size.toLong + 1) / 2

  def getMatchCount() = {
    groups.map(g => gaussSum(g._2.size - 1)).sum
  }

}
