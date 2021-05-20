package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.fact_merging.metrics.EdgeScore
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.TransitionHistogramMode.TransitionHistogramMode

class TransitionMatchScore[A](TIMESTAMP_RESOLUTION_IN_DAYS:Long,
                           histogramMode: TransitionHistogramMode) extends EdgeScore[A]{
  override def name: String = s"TransitionMatchScore_$histogramMode"

  override def compute(tr1: TupleReference[A]): Double = 0.0

  override def compute(f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]): Double = {
    new TransitionMatchScoreComputer[A](f1,f2,TIMESTAMP_RESOLUTION_IN_DAYS,histogramMode).computeScore()
  }
}
