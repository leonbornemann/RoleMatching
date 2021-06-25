package de.hpi.tfm.fact_merging.optimization.tuning

import de.hpi.tfm.evaluation.data.{GeneralEdge, IdentifiedFactLineage}
import de.hpi.tfm.fact_merging.metrics.{MultipleEventWeightScore, MultipleEventWeightScoreComputer}

import java.io.File
import java.time.LocalDate

object StatisticBasedWeightTuningMain extends App {
  val inputFile = new File(args(0))
  val dsName = args(1)
  val trainTimeEnd = LocalDate.parse(args(2))
  val edges = GeneralEdge.fromJsonObjectPerLineFile(inputFile.getAbsolutePath)
  assert(false) //TODO!!!
  //val aggregator = MultipleEventWeightScoreComputer.aggregateEventCounts(edges)

}
