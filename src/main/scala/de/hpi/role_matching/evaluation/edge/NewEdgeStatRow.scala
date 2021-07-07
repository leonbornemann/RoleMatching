package de.hpi.role_matching.evaluation.edge

import de.hpi.role_matching.compatibility.graph.representation.simple.GeneralEdge
import de.hpi.role_matching.evaluation.StatComputer
import de.hpi.role_matching.scoring.MultipleEventWeightScoreOccurrenceStats
import de.hpi.role_matching.scoring.MultipleEventWeightScoreOccurrenceStats.{NEUTRAL, STRONGNEGATIVE, STRONGPOSTIVE, WEAKNEGATIVE, WEAKPOSTIVE}
import de.hpi.socrata.tfmp_input.table.nonSketch.FactLineage

import java.time.LocalDate

case class NewEdgeStatRow(e: GeneralEdge,
                          scoreStats: MultipleEventWeightScoreOccurrenceStats,
                          evaluationStepDurationInDays:Int) extends StatComputer {

  val fl1 = e.v1.factLineage.toFactLineage
  val fl2 = e.v2.factLineage.toFactLineage
  val remainsValidFullTimeSpan = fl1.tryMergeWithConsistent(fl2, RemainsValidVariant.STRICT).isDefined
  val isInteresting = getPointInTimeOfRealChangeAfterTrainPeriod(fl1, scoreStats.trainTimeEnd).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(fl2, scoreStats.trainTimeEnd).isDefined
  val interestingnessEvidence = getEvidenceInTestPhase(fl1, fl2, scoreStats.trainTimeEnd)

  private val evalEndDateOneTimeUnitAfterTrain: LocalDate = scoreStats.trainTimeEnd.plusDays(evaluationStepDurationInDays)
  val fl1Projected: FactLineage = fl1.projectToTimeRange(fl1.firstTimestamp, evalEndDateOneTimeUnitAfterTrain)
  val fl2Projected: FactLineage = fl2.projectToTimeRange(fl2.firstTimestamp, evalEndDateOneTimeUnitAfterTrain)
  val remainsValidOneTimeUnitAfterTrain = fl1Projected.tryMergeWithConsistent(fl2Projected, RemainsValidVariant.STRICT).isDefined
  val isInterestingOneTimeUnitAfterTrain = getPointInTimeOfRealChangeAfterTrainPeriod(fl1Projected, scoreStats.trainTimeEnd).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(fl2Projected, scoreStats.trainTimeEnd).isDefined
  val interestingnessEvidenceOneTimeUnitAfterTrain = getEvidenceInTestPhase(fl1Projected, fl2Projected, scoreStats.trainTimeEnd)

  def getSchema = {
    val eventCounts = Seq(STRONGPOSTIVE, WEAKPOSTIVE, NEUTRAL, WEAKNEGATIVE, STRONGNEGATIVE).map(s => s + "_count")
    val eventScores = Seq(STRONGPOSTIVE, WEAKPOSTIVE, NEUTRAL, WEAKNEGATIVE, STRONGNEGATIVE).map(s => s + "_scoreSum")
    (Seq("Vertex1ID,Vertex2ID") ++
      Seq("trainEndDate", "remainsValidFullTimeSpan", "hasChangeAfterTrainPeriod", "interestingnessEvidence") ++
      Seq("evalEndDateOneTimeUnitAfterTrain", "remainsValidOneTimeUnitAfterTrain", "isInterestingOneTimeUnitAfterTrain", "interestingnessEvidenceOneTimeUnitAfterTrain") ++
      eventCounts ++ eventScores)
  }

  def getStatRow = {
    Seq(e.v1.csvSafeID, e.v2.csvSafeID,
      scoreStats.trainTimeEnd, remainsValidFullTimeSpan, isInteresting, interestingnessEvidence,
      evalEndDateOneTimeUnitAfterTrain,remainsValidOneTimeUnitAfterTrain,isInterestingOneTimeUnitAfterTrain,interestingnessEvidenceOneTimeUnitAfterTrain,
      scoreStats.strongPositive, scoreStats.weakPositive, scoreStats.neutral, scoreStats.weakNegative, scoreStats.strongNegative) ++ scoreStats.summedScores.get
  }


}
