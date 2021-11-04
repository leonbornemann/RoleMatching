package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

trait IterableRoleIndex extends RoleTreeUtility {
  def getParentKeyValues: IndexedSeq[Any]

  def wildcardBuckets: IndexedSeq[RoleTreePartition]

  def tupleGroupIterator(skipWildCardBuckets: Boolean): Iterator[RoleTreePartition]
}
