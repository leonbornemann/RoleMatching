package de.hpi.role_matching.cbrm.data

import java.time.LocalDate

case class ChangePoint(prevValueA: Any,
                       prevValueB: Any,
                       curValueA: Any,
                       curValueB: Any,
                       pointInTime: LocalDate,
                       prevPointInTime: LocalDate,
                       var isLast:Boolean) {
  def prevWasCompatible: Boolean = prevValueA==prevValueB || RoleLineage.isWildcard(prevValueA) || RoleLineage.isWildcard(prevValueB)

  def isCompatible: Boolean = curValueA == curValueB || RoleLineage.isWildcard(curValueA) || RoleLineage.isWildcard(curValueB)

}
