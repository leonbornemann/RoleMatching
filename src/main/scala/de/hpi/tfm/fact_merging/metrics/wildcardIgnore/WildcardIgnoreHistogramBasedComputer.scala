package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition

import java.time.LocalDate

abstract class WildcardIgnoreHistogramBasedComputer[A](f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A],TIMESTAMP_RESOLUTION_IN_DAYS:Long,
                                                       includeConsecutiveSameValueTransitions:Boolean=true) {

  def buildTransitionHistogram(f1: TemporalFieldTrait[A]) = {
    val withIndex = f1.getValueLineage
      .filter(t => !isWildcard(t._2))
      .toIndexedSeq
      .zipWithIndex
    val transitionToPeriod = withIndex
      .tail
      //.withFilter{case ((t,v),i) => i!=0}
      .map{case ((t,v),i) => {
        val ((tPrev,vPrev),iPrev) = withIndex(i-1)
        if(includeConsecutiveSameValueTransitions){
          //add tPrev as many times as we have it:
          val begin = tPrev.toEpochDay
          val end = t.minusDays(TIMESTAMP_RESOLUTION_IN_DAYS).toEpochDay
          val intervals = (begin until end by TIMESTAMP_RESOLUTION_IN_DAYS).map(begin => {
            TimeInterval(LocalDate.ofEpochDay(begin),Some(LocalDate.ofEpochDay(begin+TIMESTAMP_RESOLUTION_IN_DAYS)))
          })

        }
        val transition = ValueTransition(vPrev,v)
        val timePeriod = TimeInterval(t.minusDays(TIMESTAMP_RESOLUTION_IN_DAYS),Some(t))
        (transition,timePeriod)
      }}
      .groupMap(_._1)(_._2)
    transitionToPeriod
  }

  def isWildcard(a:A) = f1.isWildcard(a)

  //build histograms:
  val hist1 = buildTransitionHistogram(f1)
  val hist2 = buildTransitionHistogram(f2)

}
