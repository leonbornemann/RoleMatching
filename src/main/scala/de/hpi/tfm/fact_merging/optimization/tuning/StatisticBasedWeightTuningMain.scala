package de.hpi.tfm.fact_merging.optimization.tuning

import de.hpi.tfm.evaluation.data.{GeneralEdge, IdentifiedFactLineage, SlimGraphWithoutWeight}
import de.hpi.tfm.fact_merging.metrics.{MultipleEvenWeightStatCounter, MultipleEventWeightScore, MultipleEventWeightScoreComputer, MultipleEventWeightScoreOccurrenceStats}
import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate

object StatisticBasedWeightTuningMain extends App {
  val inputFile = new File(args(0))
  val resultDir = new File(args(1))
  resultDir.mkdir()
//  val lol = new File(resultDir.getAbsolutePath+"/politics/").listFiles().toIndexedSeq.map(f => MultipleEventWeightScoreOccurrenceStats.fromJsonFile(f.getAbsolutePath))
//  val res = lol.map(s => (s.trainTimeEnd,s.strongPositive / s.strongNegative.toDouble,s.weakPositive / s.weakNegative.toDouble,s.weakNegative / s.strongNegative.toDouble))
//  res.sortBy(_._1.toEpochDay).foreach(println(_))
  val TIMESTAMP_GRANULARITY_IN_DAYS = args(2).toInt
  val dataSource = args(3)
  IOService.setDatesForDataSource(dataSource)
  val dsName = inputFile.getName.split("\\.")(0)
  val graph = SlimGraphWithoutWeight.fromJsonFile(inputFile.getAbsolutePath)
  println(graph.verticesOrdered.size)
  println(graph.adjacencyList.size)
  println(graph.adjacencyList.map(_._2.size).sum)
  val aggregator = new MultipleEvenWeightStatCounter(dsName,graph,TIMESTAMP_GRANULARITY_IN_DAYS)
  val counts = aggregator.aggregateEventCounts()
  counts.values.foreach(c => {
    val dir = new File(resultDir.getAbsolutePath + s"/$dsName/")
    dir.mkdir()
    c.toJsonFile(new File(dir.getAbsolutePath + s"/${c.trainTimeEnd}.json"))
  })

}
