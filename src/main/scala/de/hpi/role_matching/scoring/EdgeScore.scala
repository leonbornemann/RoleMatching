package de.hpi.role_matching.scoring

import de.hpi.socrata.tfmp_input.table.TemporalFieldTrait
import de.hpi.role_matching.compatibility.graph.creation.TupleReference

trait EdgeScore[A] {

  def name: String

  def compute(tr1: TupleReference[A], tr2: TupleReference[A]): Double = compute(tr1.getDataTuple.head, tr2.getDataTuple.head)

  def compute(tr1: TupleReference[A]): Double

  def compute(f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]): Double

}
