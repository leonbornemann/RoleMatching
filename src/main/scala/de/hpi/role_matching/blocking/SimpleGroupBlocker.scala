package de.hpi.role_matching.blocking

import de.hpi.role_matching.cbrm.data.RoleLineage

trait SimpleGroupBlocker {

  val groups: Map[Any, Iterable[RoleLineage]]

  def gaussSum(size: Int) = size.toLong * (size.toLong + 1) / 2

  def getMatchCount() = {
    groups.map(g => gaussSum(g._2.size-1)).sum
  }

}
