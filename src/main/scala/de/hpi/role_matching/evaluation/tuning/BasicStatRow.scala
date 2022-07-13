package de.hpi.role_matching.evaluation.tuning

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.RoleLineage
import de.hpi.role_matching.evaluation.matching.StatComputer

import java.time.LocalDate

class BasicStatRow(val fl1:RoleLineage,val fl2:RoleLineage,val trainTimeEnd:LocalDate) extends StatComputer{

  def isInterestingInEvaluation = getPointInTimeOfRealChangeAfterTrainPeriod(fl1, trainTimeEnd).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(fl2, trainTimeEnd).isDefined

  def remainsValidFullTimeSpan = fl1.tryMergeWithConsistent(fl2, RemainsValidVariant.STRICT).isDefined

  def interestingnessEvidence = getEvidenceInTestPhase(fl1, fl2, trainTimeEnd)

  def isValidSuperStrict = {
    val fl1Projected = fl1.lineage.rangeFrom(trainTimeEnd).filter(t => !RoleLineage.isWildcard(t._2))
    val fl2Projected = fl2.lineage.rangeFrom(trainTimeEnd).filter(t => !RoleLineage.isWildcard(t._2))
    fl1Projected == fl2Projected
  }

  def isValidFirstChangeAfter = {
    val firstChangeA = fl1.lineage.iteratorFrom(trainTimeEnd).find{case (ld,v) => !RoleLineage.isWildcard(v)}
    val firstChangeB = fl2.lineage.iteratorFrom(trainTimeEnd).find{case (ld,v) => !RoleLineage.isWildcard(v)}
    val bothNotDefined = !firstChangeA.isDefined && !firstChangeB.isDefined
    val oneNotDefined = !firstChangeA.isDefined || !firstChangeB.isDefined
    if(bothNotDefined)
      true
    else if(oneNotDefined)
      false
    else
      firstChangeA.get==firstChangeB.get
  }
}
