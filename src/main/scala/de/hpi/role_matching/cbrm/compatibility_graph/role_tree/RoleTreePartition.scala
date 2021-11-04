package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.role_matching.cbrm.data.RoleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class RoleTreePartition(chosenTimestamps: ArrayBuffer[LocalDate],
                                valuesAtTimestamps: IndexedSeq[Any],
                                nonWildcardRoles: Iterable[RoleReference],
                                wildcardRoles: Iterable[RoleReference]) {

}
