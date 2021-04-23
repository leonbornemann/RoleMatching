package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition

abstract class WildcardIgnoreHistogramBasedComputer[A](f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]) {

  val TIMESTAMP_RESOLUTION_IN_DAYS: Long = 1

  def buildTransitionHistogram(f1: TemporalFieldTrait[A]) = {
    val withIndex = f1.getValueLineage
      .filter(t => isWildcard(t._2))
      .toIndexedSeq
      .zipWithIndex
    val transitionToPeriod = withIndex
      .tail
      //.withFilter{case ((t,v),i) => i!=0}
      .map{case ((t,v),i) => {
        val ((tPrev,vPrev),iPrev) = withIndex(i-1)
        val transition = ValueTransition(vPrev,v)
        val timePeriod = TimeInterval(tPrev.plusDays(TIMESTAMP_RESOLUTION_IN_DAYS),Some(t))
        (transition,timePeriod)
      }}
      .groupMap(_._1)(_._1)
    transitionToPeriod
  }

  def isWildcard(a:A) = f1.isWildcard(a)

  //build histograms:
  val hist1 = buildTransitionHistogram(f1)
  val hist2 = buildTransitionHistogram(f2)

}
