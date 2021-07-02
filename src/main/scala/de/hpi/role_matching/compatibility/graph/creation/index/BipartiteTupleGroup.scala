package de.hpi.role_matching.compatibility.graph.creation.index

import de.hpi.role_matching.compatibility.graph.creation.TupleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class BipartiteTupleGroup[A](chosenTimestamps:ArrayBuffer[LocalDate],
                                  valuesAtTimestamps:IndexedSeq[A],
                                  wildcardTuplesLeft:IndexedSeq[TupleReference[A]],
                                  wildcardTuplesRight:IndexedSeq[TupleReference[A]],
                                  tuplesLeft:IndexedSeq[TupleReference[A]],
                                  tuplesRight:IndexedSeq[TupleReference[A]]) {
  def totalComputationsIfPairwise = {
    tuplesLeft.size * tuplesRight.size +
      wildcardTuplesLeft.size*tuplesRight.size +
      wildcardTuplesRight.size*tuplesLeft.size +
      wildcardTuplesLeft.size * wildcardTuplesRight.size
  }

}
