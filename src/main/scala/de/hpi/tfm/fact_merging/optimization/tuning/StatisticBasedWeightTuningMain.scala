package de.hpi.tfm.fact_merging.optimization.tuning

import de.hpi.tfm.evaluation.data.{GeneralEdge, IdentifiedFactLineage, SlimGraphWithoutWeight}
import de.hpi.tfm.fact_merging.metrics.{MultipleEvenWeightStatCounter, MultipleEventWeightScore, MultipleEventWeightScoreComputer}
import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate

object StatisticBasedWeightTuningMain extends App {
  val dsName = args(0)
  val inputFile = new File(args(1))
  val resultDir = new File(args(2))
  resultDir.mkdir()
  val TIMESTAMP_GRANULARITY_IN_DAYS = args(3).toInt
  val timeStart = LocalDate.parse(args(4))
  val timeEnd = LocalDate.parse(args(5))
  IOService.STANDARD_TIME_FRAME_START=timeStart
  IOService.STANDARD_TIME_FRAME_END=timeEnd
  val graph = SlimGraphWithoutWeight.fromJsonFile(inputFile.getAbsolutePath)
  val aggregator = new MultipleEvenWeightStatCounter(dsName,graph,TIMESTAMP_GRANULARITY_IN_DAYS)
  val counts = aggregator.aggregateEventCounts()
  counts.values.foreach(c => {
    c.toJsonFile(new File(resultDir.getAbsolutePath + s"/$dsName/${c.trainTimeEnd}.json"))
  })

}
