package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import de.hpi.tfm.data.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.TransitionHistogramMode.TransitionHistogramMode
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.WildcardIgnoreHistogramBasedComputer.buildTransitionHistogram

//https://en.wikipedia.org/wiki/Jaccard_index#Generalized_Jaccard_similarity_and_distance
class RuzickaDistanceComputer[A](f1: TemporalFieldTrait[A],
                                 f2: TemporalFieldTrait[A],
                                 TIMESTAMP_RESOLUTION_IN_DAYS:Long,
                                 histogramMode: TransitionHistogramMode,
                                 hist1:Map[ValueTransition[A], IndexedSeq[TimeInterval]],
                                 hist2:Map[ValueTransition[A], IndexedSeq[TimeInterval]])
  extends WildcardIgnoreHistogramBasedComputer[A](f1,f2,TIMESTAMP_RESOLUTION_IN_DAYS,histogramMode,hist1,hist2){

  def this(f1: TemporalFieldTrait[A],
           f2: TemporalFieldTrait[A],
           TIMESTAMP_RESOLUTION_IN_DAYS:Long,
           transitionHistogramMode:TransitionHistogramMode) {
    this(f1,
      f2,
      TIMESTAMP_RESOLUTION_IN_DAYS,
      transitionHistogramMode,
      buildTransitionHistogram(f1,transitionHistogramMode,TIMESTAMP_RESOLUTION_IN_DAYS),
      buildTransitionHistogram(f2,transitionHistogramMode,TIMESTAMP_RESOLUTION_IN_DAYS))
  }


  def computeScore(): Double = {
    val (nominator,denominator) = hist1.keySet.union(hist2.keySet)
      .map(t => {
        val countA = hist1.getOrElse(t,Seq()).size
        val countB = hist2.getOrElse(t,Seq()).size
        (Math.min(countA,countB),Math.max(countA,countB))
      })
      .reduce((a,b) => (a._1+b._1,a._2+b._2))
    nominator / denominator.toDouble
  }


}
