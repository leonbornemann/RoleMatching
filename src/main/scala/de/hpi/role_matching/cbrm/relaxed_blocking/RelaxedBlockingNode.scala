package de.hpi.role_matching.cbrm.relaxed_blocking

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.{RoleLineage, RoleReference}

import java.time.LocalDate
import scala.util.Random

class RelaxedBlockingNode(references: IndexedSeq[RoleReference], trainTimeEnd:LocalDate,targetPercentage:Double,random:Random) {

  def getSplitTimestamp() = {
    GLOBAL_CONFIG.STANDARD_TIME_RANGE(random.nextInt(GLOBAL_CONFIG.STANDARD_TIME_RANGE.size))
  }

  val splitTimestamp = getSplitTimestamp()
  val representativeWildcardValue = RoleLineage.WILDCARD_VALUES.head



  val children = references.groupBy(tr => getValueAt(tr,splitTimestamp))
    .map{case (k,lineages) => new RelaxedBlockingChildNode(k,splitTimestamp,lineages,trainTimeEnd,targetPercentage)}

  val prunable = children
    .map(rbcn => (rbcn.prunable.size))
    .sum

  val nonPrunable = children
    .map(rbcn => (rbcn.nonPrunable.size))
    .sum

  private def getValueAt(tr: RoleReference,timestamp:LocalDate) = {
    val value = tr.getRole.valueAt(timestamp)
    if(RoleLineage.isWildcard(value))
      representativeWildcardValue
    else
      value
  }
}
