package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class RoleTreePartition[A](chosenTimestamps: ArrayBuffer[LocalDate],
                                valuesAtTimestamps: IndexedSeq[A],
                                nonWildcardRoles: Iterable[RoleReference[A]],
                                wildcardRoles: Iterable[RoleReference[A]]) {

}
