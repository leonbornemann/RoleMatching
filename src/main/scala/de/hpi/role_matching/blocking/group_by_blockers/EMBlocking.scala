package de.hpi.role_matching.blocking.group_by_blockers

import de.hpi.role_matching.data.Roleset

import java.time.LocalDate

class EMBlocking(roleset: Roleset, trainTimeEnd:LocalDate) extends SimpleGroupByBlocker{

  val groups = roleset.posToRoleLineage.values.groupBy(_.toExactValueSequence(trainTimeEnd))
  println()

  def idGroups:Map[IndexedSeq[Any], IndexedSeq[String]] = {
    roleset
      .getStringToLineageMap
      .groupBy { case (k, rlWID) =>
        rlWID.roleLineage
          .toRoleLineage
          .toExactValueSequence(trainTimeEnd)
      }
      .map(t => (t._1, t._2.keySet.toIndexedSeq.sorted))
  }

}
