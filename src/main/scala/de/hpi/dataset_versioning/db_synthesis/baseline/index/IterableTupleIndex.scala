package de.hpi.dataset_versioning.db_synthesis.baseline.index

trait IterableTupleIndex[T] extends TupleIndexUtility[T]{

  def tupleGroupIterator(skipWildCardBuckets: Boolean): Iterator[TupleGroup[T]]
}
