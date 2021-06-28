package de.hpi.tfm.fact_merging.optimization.tuning

import de.hpi.tfm.evaluation.data.{GeneralEdge, IdentifiedFactLineage, SlimGraphWithoutWeight}
import de.hpi.tfm.fact_merging.metrics.{MultipleEvenWeightStatCounter, MultipleEventWeightScore, MultipleEventWeightScoreComputer}

import java.io.File
import java.time.LocalDate

object StatisticBasedWeightTuningMain extends App {
  val dsName = args(0)
  val inputFile = new File(args(1))
  val resultDir = new File(args(2))
  resultDir.mkdir()
  val TIMESTAMP_GRANULARITY_IN_DAYS = args(3).toInt
  val graph = SlimGraphWithoutWeight.fromJsonFile(inputFile.getAbsolutePath)
  val aggregator = new MultipleEvenWeightStatCounter(dsName,graph,TIMESTAMP_GRANULARITY_IN_DAYS)
  val counts = aggregator.aggregateEventCounts()
  counts.values.foreach(c => {
    c.toJsonFile(new File(resultDir.getAbsolutePath + s"/$dsName/${c.trainTimeEnd}.json"))
  })

}
