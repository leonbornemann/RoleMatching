package de.hpi.role_matching.blocking.cbrb.role_tree.normal

import de.hpi.role_matching.data.RoleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class RoleTreePartition(chosenTimestamps: ArrayBuffer[LocalDate],
                             valuesAtTimestamps: IndexedSeq[Any],
                             nonWildcardRoles: Iterable[RoleReference],
                             wildcardRoles: Iterable[RoleReference]) {

}
