package de.hpi.role_matching.cbrm.relaxed_blocking

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.{RoleLineage, RoleReference}

import java.time.LocalDate
import java.time.temporal.ChronoUnit

class RelaxedBlockingChildNode(splitValue:Any,
                               splitTimestamp: LocalDate,
                               lineages: IndexedSeq[RoleReference],
                               trainTimeEnd:LocalDate,
                               targetPercentage:Double) {

  def sortByTimeAfterSplit(lineages: IndexedSeq[RoleReference]) = {
    lineages.map(l => {
      val nextChangeInDays = ChronoUnit.DAYS.between(splitTimestamp,l.getRole.lineage.keysIteratorFrom(splitTimestamp.plusDays(1)).nextOption().getOrElse(trainTimeEnd))
      val percentageOfTime = nextChangeInDays / ChronoUnit.DAYS.between(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd).toDouble
      (l,percentageOfTime)
    })
      .sortBy(_._2)
  }

  def allowedError = 1.0 - targetPercentage

  val lineagesSorted = sortByTimeAfterSplit(lineages)
  val prunable = if(RoleLineage.isWildcard(splitValue)) IndexedSeq() else lineagesSorted.takeWhile(_._2>=allowedError)
  val nonPrunable = if(RoleLineage.isWildcard(splitValue)) lineagesSorted else lineagesSorted.slice(prunable.size,lineagesSorted.size)
}
