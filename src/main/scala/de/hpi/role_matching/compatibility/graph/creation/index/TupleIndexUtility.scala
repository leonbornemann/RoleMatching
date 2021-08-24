package de.hpi.role_matching.compatibility.graph.creation.index

import de.hpi.role_matching.GLOBAL_CONFIG
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

  def getRandomSample[B](tuples: IndexedSeq[B],samplingRate:Double): IndexedSeq[B] = {
    val resultSize = tuples.size*samplingRate.round.toInt
    if(tuples.size<2 || resultSize==0)
      tuples
    else {
      GLOBAL_CONFIG.random.shuffle(tuples).take(resultSize)
    }
  }

  def getField(tupleReference: TupleReference[A]) = tupleReference.table
    .getDataTuple(tupleReference.rowIndex).head

}
