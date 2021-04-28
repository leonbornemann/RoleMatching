package de.hpi.tfm.fact_merging.metrics

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait

trait EdgeScore {

  def name:String
  def compute[A](tr1: TupleReference[A], tr2: TupleReference[A]) :Double = compute(tr1.getDataTuple.head,tr2.getDataTuple.head)
  def compute[A](tr1: TupleReference[A]): Double
  def compute[A](f1:TemporalFieldTrait[A],f2:TemporalFieldTrait[A]):Double

}
