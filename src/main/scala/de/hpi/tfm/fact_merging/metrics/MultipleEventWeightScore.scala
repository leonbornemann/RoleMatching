package de.hpi.tfm.fact_merging.metrics
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait

class MultipleEventWeightScore() extends EdgeScore {
  override def name: String = MultipleEventWeightScore.name

  override def compute[A](tr1: TupleReference[A], tr2: TupleReference[A]): Double =
    new MultipleEventWeightScoreComputer[A](tr1.getDataTuple.head,tr2.getDataTuple.head).score()

  override def compute[A](tr1: TupleReference[A]): Double =
    MultipleEventWeightScoreComputer.scoreOfSingletonVertex

  override def compute[A](f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]): Double =
    new MultipleEventWeightScoreComputer[A](f1,f2).score()
}
object MultipleEventWeightScore{
  val name = "MultipleEventWeightScore"
}
