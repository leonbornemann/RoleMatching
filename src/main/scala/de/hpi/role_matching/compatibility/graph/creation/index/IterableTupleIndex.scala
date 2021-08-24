package de.hpi.role_matching.compatibility.graph.creation.index

import de.hpi.role_matching.GLOBAL_CONFIG

trait IterableTupleIndex[T] extends TupleIndexUtility[T]{
  def getParentKeyValues:IndexedSeq[T]

  def wildcardBuckets: IndexedSeq[TupleGroup[T]]

  def tupleGroupIterator(skipWildCardBuckets: Boolean): Iterator[TupleGroup[T]]
}
