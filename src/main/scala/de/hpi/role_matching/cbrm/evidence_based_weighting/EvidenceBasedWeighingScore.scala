package de.hpi.role_matching.cbrm.evidence_based_weighting

import de.hpi.role_matching.cbrm.data.{RoleLineage, ValueTransition}

import java.time.LocalDate

class EvidenceBasedWeighingScore(TIMESTAMP_GRANULARITY_IN_DAYS:Int,
                                 timeEnd:LocalDate,
                                 nonInformativeValues:Set[Any],
                                 nonInformativeValueIsStrict:Boolean, //true if it is enough for one value in a transition to be non-informative to discard it, false if both of them need to be non-informative to discard it
                                 transitionHistogramForTFIDF:Option[Map[ValueTransition,Int]],
                                 lineageCount:Option[Int]) extends EdgeScore {

  val baseName = "MultipleEventWeightScore"

  override def name: String = {
    baseName +
      (if(nonInformativeValueIsStrict) "_NIONE" else "_NITWO") +
      (if(transitionHistogramForTFIDF.isDefined) "_tfwON" else "_tfwOFF")
  }

  def compute(tr1: RoleLineage): Double =
    EvidenceBasedWeightingScoreComputer.scoreOfSingletonVertex

  override def compute(f1: RoleLineage, f2: RoleLineage): Double =
    new EvidenceBasedWeightingScoreComputer(f1,
      f2,
      TIMESTAMP_GRANULARITY_IN_DAYS,
      timeEnd,
      nonInformativeValues,
      nonInformativeValueIsStrict,
      transitionHistogramForTFIDF,
      lineageCount).score()
}
object EvidenceBasedWeighingScore{
  val name = "MultipleEventWeightScore"
}
