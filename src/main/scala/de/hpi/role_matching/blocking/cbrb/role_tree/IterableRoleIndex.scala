package de.hpi.role_matching.blocking.cbrb.role_tree

import de.hpi.role_matching.blocking.cbrb.role_tree.normal.RoleTreePartition

trait IterableRoleIndex extends RoleTreeUtility {
  def getParentKeyValues: IndexedSeq[Any]

  def wildcardBuckets: IndexedSeq[RoleTreePartition]

  def tupleGroupIterator(skipWildCardBuckets: Boolean): Iterator[RoleTreePartition]
}
