package de.hpi.role_matching.evaluation.edge

import de.hpi.role_matching.compatibility.graph.representation.simple.GeneralEdge
import de.hpi.role_matching.evaluation.StatComputer
import de.hpi.role_matching.scoring.MultipleEventWeightScoreOccurrenceStats
import de.hpi.role_matching.scoring.MultipleEventWeightScoreOccurrenceStats.{NEUTRAL, STRONGNEGATIVE, STRONGPOSTIVE, WEAKNEGATIVE, WEAKPOSTIVE}

case class NewEdgeStatRow(e: GeneralEdge,
                          scoreStats: MultipleEventWeightScoreOccurrenceStats) extends StatComputer {
  val fl1 = e.v1.factLineage.toFactLineage
  val fl2 = e.v2.factLineage.toFactLineage
  val remainsValidStrict = fl1.tryMergeWithConsistent(fl2, RemainsValidVariant.STRICT).isDefined
  val isInteresting = getPointInTimeOfRealChangeAfterTrainPeriod(fl1, scoreStats.trainTimeEnd).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(fl2, scoreStats.trainTimeEnd).isDefined
  val interestingnessEvidence = getEvidenceInTestPhase(fl1, fl2, scoreStats.trainTimeEnd)

  def getSchema = {
    val eventCounts = Seq(STRONGPOSTIVE, WEAKPOSTIVE, NEUTRAL, WEAKNEGATIVE, STRONGNEGATIVE).map(s => s + "_count")
    val eventScores = Seq(STRONGPOSTIVE, WEAKPOSTIVE, NEUTRAL, WEAKNEGATIVE, STRONGNEGATIVE).map(s => s + "_scoreSum")
    Seq("Vertex1ID,Vertex2ID") ++ Seq("trainEndDate", "remainsValid", "hasChangeAfterTrainPeriod", "interestingnessEvidence") ++ eventCounts ++ eventScores
  }

  def getStatRow = {
    Seq(e.v1.id, e.v2.id, scoreStats.trainTimeEnd, remainsValidStrict, isInteresting, interestingnessEvidence,
      scoreStats.strongPositive, scoreStats.weakPositive, scoreStats.neutral, scoreStats.weakNegative, scoreStats.strongNegative) ++ scoreStats.summedScores.get
  }


}
