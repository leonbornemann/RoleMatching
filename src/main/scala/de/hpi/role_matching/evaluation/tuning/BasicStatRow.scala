package de.hpi.role_matching.evaluation.tuning

import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.RoleLineage
import de.hpi.role_matching.evaluation.matching.StatComputer

import java.time.LocalDate

class BasicStatRow(val fl1:RoleLineage,val fl2:RoleLineage,val trainTimeEnd:LocalDate) extends StatComputer{

  def isInteresting = getPointInTimeOfRealChangeAfterTrainPeriod(fl1, trainTimeEnd).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(fl2, trainTimeEnd).isDefined

  def remainsValidFullTimeSpan = fl1.tryMergeWithConsistent(fl2, RemainsValidVariant.STRICT).isDefined

  def interestingnessEvidence = getEvidenceInTestPhase(fl1, fl2, trainTimeEnd)
}
