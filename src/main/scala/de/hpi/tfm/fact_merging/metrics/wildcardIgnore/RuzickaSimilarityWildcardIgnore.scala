package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.fact_merging.metrics.EdgeScore

class RuzickaSimilarityWildcardIgnore extends EdgeScore {
  override def name: String = "RuzickaSimilarityWildcardIgnore"

  override def compute[A](tr1: TupleReference[A], tr2: TupleReference[A]): Double = {
    val scoreComputer = new RuzickaDistanceComputer(tr1,tr2)
    scoreComputer.computeScore()
  }

  override def compute[A](tr1: TupleReference[A]): Double = 0.0
}
