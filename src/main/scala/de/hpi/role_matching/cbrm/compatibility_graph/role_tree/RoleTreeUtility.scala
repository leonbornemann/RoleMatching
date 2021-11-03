package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.role_matching.GLOBAL_CONFIG

trait RoleTreeUtility[A] {

  def getRelevantTimestamps(tuples: IndexedSeq[RoleReference[A]]) = {
    tuples
      .map(r => {
        val a = getField(r)
        a.allTimestamps.toSet
      }).flatten
      .toSet
  }

  def getRandomSample[B](tuples: IndexedSeq[B], samplingRate: Double): IndexedSeq[B] = {
    val resultSize = tuples.size * samplingRate.round.toInt
    if (tuples.size < 2 || resultSize == 0)
      tuples
    else {
      GLOBAL_CONFIG.random.shuffle(tuples).take(resultSize)
    }
  }

  def getField(tupleReference: RoleReference[A]) = tupleReference.table
    .getDataTuple(tupleReference.rowIndex).head

}
