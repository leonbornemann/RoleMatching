package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.db_synthesis.baseline.index.TupleGroup

trait IterableTupleIndex[T] {

  def tupleGroupIterator:Iterator[TupleGroup[T]]
}
