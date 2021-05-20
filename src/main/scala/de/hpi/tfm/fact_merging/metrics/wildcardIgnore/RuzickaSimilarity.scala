package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.fact_merging.metrics.EdgeScore
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.TransitionHistogramMode.TransitionHistogramMode

class RuzickaSimilarity[A](TIMESTAMP_RESOLUTION_IN_DAYS:Long, histogramMode: TransitionHistogramMode) extends EdgeScore[A] {
  override def name: String = s"RuzickaSimilarity_$histogramMode"

  override def compute(tr1: TupleReference[A], tr2: TupleReference[A]): Double = {
    compute(tr1.getDataTuple.head,tr2.getDataTuple.head)
  }

  override def compute(tr1: TupleReference[A]): Double = 0.0

  override def compute(f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]): Double = {
    new RuzickaDistanceComputer(f1,f2,TIMESTAMP_RESOLUTION_IN_DAYS,histogramMode).computeScore()
  }
}
