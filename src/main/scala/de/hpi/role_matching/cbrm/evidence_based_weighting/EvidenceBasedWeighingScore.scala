package de.hpi.role_matching.cbrm.evidence_based_weighting

import de.hpi.data_preparation.socrata.tfmp_input.table.TemporalFieldTrait
import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch.ValueTransition
import .TFIDFWeightingVariant
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.RoleReference

import java.time.LocalDate

class EvidenceBasedWeighingScore[A](TIMESTAMP_GRANULARITY_IN_DAYS:Int,
                                    timeEnd:LocalDate,
                                    nonInformativeValues:Set[A],
                                    nonInformativeValueIsStrict:Boolean, //true if it is enough for one value in a transition to be non-informative to discard it, false if both of them need to be non-informative to discard it
                                    transitionHistogramForTFIDF:Option[Map[ValueTransition[A],Int]],
                                    lineageCount:Option[Int],
                                    tfidfWeightingOption:Option[TFIDFWeightingVariant]) extends EdgeScore[A] {
  override def name: String = {
    EvidenceBasedWeighingScore.name +
      (if(nonInformativeValueIsStrict) "_NIONE" else "_NITWO") +
      (if(transitionHistogramForTFIDF.isDefined) "_tfwON" else "_tfwOFF") +
      (if(tfidfWeightingOption.isDefined) "_" + tfidfWeightingOption.get else "_LIN")
  }

  override def compute(tr1: RoleReference[A], tr2: RoleReference[A]): Double =
    new EvidenceBasedWeightingScoreComputer[A](tr1.getDataTuple.head,
      tr2.getDataTuple.head,
      TIMESTAMP_GRANULARITY_IN_DAYS,
      timeEnd,
      nonInformativeValues,
      nonInformativeValueIsStrict,
      transitionHistogramForTFIDF,
      lineageCount,
      tfidfWeightingOption).score()

  override def compute(tr1: RoleReference[A]): Double =
    EvidenceBasedWeightingScoreComputer.scoreOfSingletonVertex

  override def compute(f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]): Double =
    new EvidenceBasedWeightingScoreComputer[A](f1,
      f2,
      TIMESTAMP_GRANULARITY_IN_DAYS,
      timeEnd,
      nonInformativeValues,
      nonInformativeValueIsStrict,
      transitionHistogramForTFIDF,
      lineageCount,
      tfidfWeightingOption).score()
}
object EvidenceBasedWeighingScore{
  val name = "MultipleEventWeightScore"
}
