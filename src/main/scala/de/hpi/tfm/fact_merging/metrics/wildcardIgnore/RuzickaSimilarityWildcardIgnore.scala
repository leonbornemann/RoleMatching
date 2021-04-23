package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.fact_merging.metrics.EdgeScore

class RuzickaSimilarityWildcardIgnore extends EdgeScore {
  override def name: String = "RuzickaSimilarityWildcardIgnore"

  override def compute[A](tr1: TupleReference[A], tr2: TupleReference[A]): Double = {
    compute(tr1.getDataTuple.head,tr2.getDataTuple.head)
  }

  override def compute[A](tr1: TupleReference[A]): Double = 0.0

  override def compute[A](f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]): Double =
    new RuzickaDistanceComputer(f1,f2).computeScore()
}
