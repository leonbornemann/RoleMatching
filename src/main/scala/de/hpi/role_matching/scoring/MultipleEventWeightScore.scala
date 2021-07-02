package de.hpi.role_matching.scoring

import de.hpi.socrata.tfmp_input.table.TemporalFieldTrait
import de.hpi.socrata.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.role_matching.compatibility.graph.creation.TupleReference
import de.hpi.role_matching.scoring.TFIDFWeightingVariant.TFIDFWeightingVariant

import java.time.LocalDate

class MultipleEventWeightScore[A](TIMESTAMP_GRANULARITY_IN_DAYS:Int,
                               timeEnd:LocalDate,
                               nonInformativeValues:Set[A],
                               nonInformativeValueIsStrict:Boolean, //true if it is enough for one value in a transition to be non-informative to discard it, false if both of them need to be non-informative to discard it
                               transitionHistogramForTFIDF:Option[Map[ValueTransition[A],Int]],
                               lineageCount:Option[Int],
                               tfidfWeightingOption:Option[TFIDFWeightingVariant]) extends EdgeScore[A] {
  override def name: String = {
    MultipleEventWeightScore.name +
      (if(nonInformativeValueIsStrict) "_NIONE" else "_NITWO") +
      (if(transitionHistogramForTFIDF.isDefined) "_tfwON" else "_tfwOFF") +
      (if(tfidfWeightingOption.isDefined) "_" + tfidfWeightingOption.get else "_LIN")
  }

  override def compute(tr1: TupleReference[A], tr2: TupleReference[A]): Double =
    new MultipleEventWeightScoreComputer[A](tr1.getDataTuple.head,
      tr2.getDataTuple.head,
      TIMESTAMP_GRANULARITY_IN_DAYS,
      timeEnd,
      nonInformativeValues,
      nonInformativeValueIsStrict,
      transitionHistogramForTFIDF,
      lineageCount,
      tfidfWeightingOption).score()

  override def compute(tr1: TupleReference[A]): Double =
    MultipleEventWeightScoreComputer.scoreOfSingletonVertex

  override def compute(f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]): Double =
    new MultipleEventWeightScoreComputer[A](f1,
      f2,
      TIMESTAMP_GRANULARITY_IN_DAYS,
      timeEnd,
      nonInformativeValues,
      nonInformativeValueIsStrict,
      transitionHistogramForTFIDF,
      lineageCount,
      tfidfWeightingOption).score()
}
object MultipleEventWeightScore{
  val name = "MultipleEventWeightScore"
}
