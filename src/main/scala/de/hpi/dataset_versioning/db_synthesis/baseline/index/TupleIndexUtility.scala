package de.hpi.dataset_versioning.db_synthesis.baseline.index

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

trait TupleIndexUtility[A] {

  def getRelevantTimestamps(tuples:IndexedSeq[TupleReference[A]]) = {
    tuples
      .map(r => {
        val a = getField(r)
        a.allTimestamps.toSet
      }).flatten
      .toSet
  }

  def getField(tupleReference: TupleReference[A]) = tupleReference.table
    .getDataTuple(tupleReference.rowIndex).head

}
