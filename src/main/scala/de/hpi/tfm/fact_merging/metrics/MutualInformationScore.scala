package de.hpi.tfm.fact_merging.metrics
import de.hpi.tfm.compatibility.graph.fact.TupleReference

class MutualInformationScore extends EdgeScore {
  override def name: String = MutualInformationScore.name

  override def compute[A](tr1: TupleReference[A], tr2: TupleReference[A]): Double =
    new MutualInformationComputer[A](tr1.getDataTuple.head,tr2.getDataTuple.head).mutualInfo()
}
object MutualInformationScore{
  val name = "MutualInformationScore"
}
