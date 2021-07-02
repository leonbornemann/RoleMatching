package de.hpi.role_matching.compatibility.graph.creation.index

import de.hpi.role_matching.compatibility.graph.creation.TupleReference

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
