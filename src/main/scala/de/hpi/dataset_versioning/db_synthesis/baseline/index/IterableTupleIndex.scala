package de.hpi.dataset_versioning.db_synthesis.baseline.index

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

trait IterableTupleIndex[T] extends TupleIndexUtility[T]{
  def wildcardBuckets: IndexedSeq[TupleGroup[T]]


  def tupleGroupIterator(skipWildCardBuckets: Boolean): Iterator[TupleGroup[T]]
}
