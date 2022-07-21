package de.hpi.role_matching.blocking.cbrb.role_tree

import de.hpi.role_matching.data.RoleReference
import de.hpi.util.GLOBAL_CONFIG

trait RoleTreeUtility {

  def getRelevantTimestamps(tuples: IndexedSeq[RoleReference]) = {
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

  def getField(tupleReference: RoleReference) = tupleReference.getRole

}
