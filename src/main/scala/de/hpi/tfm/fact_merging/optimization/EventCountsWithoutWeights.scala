package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScoreOccurrenceStats

//counts are floats because of tf-idf
case class EventCountsWithoutWeights(var strongPositive:Float,
                                     var weakPositive:Float,
                                     var neutral:Float,
                                     var weakNegative:Float,
                                     var strongNegative:Float) {

}

object EventCountsWithoutWeights {
  def from(counts: MultipleEventWeightScoreOccurrenceStats) = {
    val summedScores = counts.summedScores.get
    EventCountsWithoutWeights(summedScores(0),summedScores(1),summedScores(2),summedScores(3),summedScores(4))
  }
}
