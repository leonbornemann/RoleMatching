package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

trait IterableRoleIndex[T] extends RoleTreeUtility[T] {
  def getParentKeyValues: IndexedSeq[T]

  def wildcardBuckets: IndexedSeq[RoleTreePartition[T]]

  def tupleGroupIterator(skipWildCardBuckets: Boolean): Iterator[RoleTreePartition[T]]
}
