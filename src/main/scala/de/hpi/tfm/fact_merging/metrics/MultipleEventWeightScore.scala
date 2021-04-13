package de.hpi.tfm.fact_merging.metrics
import de.hpi.tfm.compatibility.graph.fact.TupleReference

class MultipleEventWeightScore() extends EdgeScore {
  override def name: String = MultipleEventWeightScore.name

  override def compute[A](tr1: TupleReference[A], tr2: TupleReference[A]): Double =
    new MultipleEventWeightScoreComputer[A](tr1.getDataTuple.head,tr2.getDataTuple.head).score()

  override def compute[A](tr1: TupleReference[A]): Double =
    MultipleEventWeightScoreComputer.scoreOfSingletonVertex
}
object MultipleEventWeightScore{
  val name = "MultipleEventWeightScore"
}
