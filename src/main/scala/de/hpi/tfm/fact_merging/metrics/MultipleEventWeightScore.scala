package de.hpi.tfm.fact_merging.metrics
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait

import java.time.LocalDate

class MultipleEventWeightScore(TIMESTAMP_GRANULARITY_IN_DAYS:Int,timeEnd:LocalDate) extends EdgeScore {
  override def name: String = MultipleEventWeightScore.name

  override def compute[A](tr1: TupleReference[A], tr2: TupleReference[A]): Double =
    new MultipleEventWeightScoreComputer[A](tr1.getDataTuple.head,tr2.getDataTuple.head,TIMESTAMP_GRANULARITY_IN_DAYS,timeEnd).score()

  override def compute[A](tr1: TupleReference[A]): Double =
    MultipleEventWeightScoreComputer.scoreOfSingletonVertex

  override def compute[A](f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A]): Double =
    new MultipleEventWeightScoreComputer[A](f1,f2,TIMESTAMP_GRANULARITY_IN_DAYS,timeEnd).score()
}
object MultipleEventWeightScore{
  val name = "MultipleEventWeightScore"
}
