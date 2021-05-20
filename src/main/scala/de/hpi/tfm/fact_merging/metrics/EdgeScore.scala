package de.hpi.tfm.fact_merging.metrics

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait

trait EdgeScore[A] {

  def name:String
  def compute(tr1: TupleReference[A], tr2: TupleReference[A]) :Double = compute(tr1.getDataTuple.head,tr2.getDataTuple.head)
  def compute(tr1: TupleReference[A]): Double
  def compute(f1:TemporalFieldTrait[A],f2:TemporalFieldTrait[A]):Double

}
