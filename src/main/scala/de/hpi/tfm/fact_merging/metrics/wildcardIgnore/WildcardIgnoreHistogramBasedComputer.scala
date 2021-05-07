package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.TransitionHistogramMode.TransitionHistogramMode

import java.time.LocalDate

abstract class WildcardIgnoreHistogramBasedComputer[A](val f1: TemporalFieldTrait[A],
                                                       val f2: TemporalFieldTrait[A],
                                                       val TIMESTAMP_RESOLUTION_IN_DAYS:Long,
                                                       val transitionHistogramMode:TransitionHistogramMode) {

  def buildTransitionHistogram(f1: TemporalFieldTrait[A]) = {
    val withIndex = f1.getValueLineage
      .filter(t => !isWildcard(t._2))
      .toIndexedSeq
      .zipWithIndex
    val transitionToPeriod = withIndex
      .tail
      //.withFilter{case ((t,v),i) => i!=0}
      .flatMap{case ((t,v),i) => {
        val ((tPrev,vPrev),iPrev) = withIndex(i-1)
        val transitions = collection.mutable.ArrayBuffer[(ValueTransition[A],TimeInterval)]()
        if(transitionHistogramMode != TransitionHistogramMode.IGNORE_NON_CHANGE){
          val begin = tPrev.toEpochDay
          val end = t.toEpochDay
          val prevTransition = ValueTransition(vPrev,vPrev)
          if(transitionHistogramMode ==TransitionHistogramMode.NORMAL){
            //add tPrev as many times as we have it:
            val intervals = (begin until end by TIMESTAMP_RESOLUTION_IN_DAYS).map(begin => {
              (prevTransition,TimeInterval(LocalDate.ofEpochDay(begin),Some(LocalDate.ofEpochDay(begin+TIMESTAMP_RESOLUTION_IN_DAYS).minusDays(1))))
            })
            transitions ++= intervals
          } else {
            assert(transitionHistogramMode == TransitionHistogramMode.COUNT_NON_CHANGE_ONLY_ONCE)
            transitions.append((prevTransition,TimeInterval(LocalDate.ofEpochDay(begin),Some(LocalDate.ofEpochDay(end).minusDays(1)))))
          }
        }
        val transition = ValueTransition(vPrev,v)
        val timePeriod = TimeInterval(t.minusDays(TIMESTAMP_RESOLUTION_IN_DAYS),Some(t.minusDays(1)))
        transitions.append((transition,timePeriod))
        transitions
      }}
      .groupMap(_._1)(_._2)
    transitionToPeriod
  }

  def isWildcard(a:A) = f1.isWildcard(a)

  //build histograms:
  val hist1 = buildTransitionHistogram(f1)
  val hist2 = buildTransitionHistogram(f2)

}
