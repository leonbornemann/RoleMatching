package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.fact_merging.metrics.EdgeScore

class DummyScoringFunction() extends EdgeScore[Any]{
  override def name: String = "Dummy"

  override def compute(tr1: TupleReference[Any]): Double = -1.0

  override def compute(f1: TemporalFieldTrait[Any], f2: TemporalFieldTrait[Any]): Double = -10.0
}
