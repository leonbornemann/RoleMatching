package de.hpi.dataset_versioning.db_synthesis.baseline.index

trait IterableTupleIndex[T] {

  def tupleGroupIterator: Iterator[TupleGroup[T]]
}
