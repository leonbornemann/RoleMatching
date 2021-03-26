package de.hpi.tfm.compatibility.index

import de.hpi.tfm.compatibility.graph.fact.TupleReference

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
