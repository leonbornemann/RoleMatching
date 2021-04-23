package de.hpi.tfm.fact_merging.metrics
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait

class MutualInformationScore extends EdgeScore {
  override def name: String = MutualInformationScore.name

  override def compute[A](tr1: TupleReference[A], tr2: TupleReference[A]): Double =
    compute(tr1.getDataTuple.head,tr2.getDataTuple.head)

  override def compute[A](tr1: TupleReference[A]): Double = 0.0

  override def compute[A](f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]): Double =
    new MutualInformationComputer[A](f1,f2).mutualInfo()
}
object MutualInformationScore{
  val name = "MutualInformationScore"
}
