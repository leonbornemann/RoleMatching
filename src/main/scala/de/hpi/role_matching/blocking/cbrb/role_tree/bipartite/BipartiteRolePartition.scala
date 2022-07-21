package de.hpi.role_matching.blocking.cbrb.role_tree.bipartite

import de.hpi.role_matching.data.RoleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class BipartiteRolePartition(chosenTimestamps: ArrayBuffer[LocalDate],
                                  valuesAtTimestamps: IndexedSeq[Any],
                                  wildcardTuplesLeft: IndexedSeq[RoleReference],
                                  wildcardTuplesRight: IndexedSeq[RoleReference],
                                  tuplesLeft: IndexedSeq[RoleReference],
                                  tuplesRight: IndexedSeq[RoleReference]) {
  def totalComputationsIfPairwise = {
    tuplesLeft.size * tuplesRight.size +
      wildcardTuplesLeft.size * tuplesRight.size +
      wildcardTuplesRight.size * tuplesLeft.size +
      wildcardTuplesLeft.size * wildcardTuplesRight.size
  }

}
