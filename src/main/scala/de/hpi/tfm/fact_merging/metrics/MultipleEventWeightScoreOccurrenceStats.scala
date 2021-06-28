package de.hpi.tfm.fact_merging.metrics

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}

import java.time.LocalDate

case class MultipleEventWeightScoreOccurrenceStats(val dsName:String,
                                                   val trainTimeEnd:LocalDate,
                                              var strongPositive:Int=0,
                                              var weakPositive:Int=0,
                                              var neutral:Int=0,
                                              var weakNegative:Int=0,
                                              var strongNegative:Int=0) extends JsonWritable[MultipleEventWeightScoreOccurrenceStats]{
  def addAll(countPrev: MultipleEventWeightScoreOccurrenceStats) = {
    strongPositive +=countPrev.strongPositive
    weakPositive +=countPrev.weakPositive
    neutral +=countPrev.neutral
    weakNegative +=countPrev.weakNegative
    strongNegative +=countPrev.strongNegative
  }
}
object MultipleEventWeightScoreOccurrenceStats extends JsonReadable[MultipleEventWeightScoreOccurrenceStats]
