package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class BipartiteRolePartition[A](chosenTimestamps: ArrayBuffer[LocalDate],
                                     valuesAtTimestamps: IndexedSeq[A],
                                     wildcardTuplesLeft: IndexedSeq[RoleReference[A]],
                                     wildcardTuplesRight: IndexedSeq[RoleReference[A]],
                                     tuplesLeft: IndexedSeq[RoleReference[A]],
                                     tuplesRight: IndexedSeq[RoleReference[A]]) {
  def totalComputationsIfPairwise = {
    tuplesLeft.size * tuplesRight.size +
      wildcardTuplesLeft.size * tuplesRight.size +
      wildcardTuplesRight.size * tuplesLeft.size +
      wildcardTuplesLeft.size * wildcardTuplesRight.size
  }

}
