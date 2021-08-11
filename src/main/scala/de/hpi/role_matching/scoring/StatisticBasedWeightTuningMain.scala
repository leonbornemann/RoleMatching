package de.hpi.role_matching.scoring

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.compatibility.graph.representation.slim.SlimGraphWithoutWeight

import java.io.File
import java.time.LocalDate

object StatisticBasedWeightTuningMain extends App with StrictLogging {
  logger.debug(s"Called with $args")
  val inputFile = new File(args(0))
  val resultDir = new File(args(1))
  val tfIDFRootDir = new File(args(2))
  resultDir.mkdir()
  //  val lol = new File(resultDir.getAbsolutePath+"/politics/").listFiles().toIndexedSeq.map(f => MultipleEventWeightScoreOccurrenceStats.fromJsonFile(f.getAbsolutePath))
  //  val res = lol.map(s => (s.trainTimeEnd,s.strongPositive / s.strongNegative.toDouble,s.weakPositive / s.weakNegative.toDouble,s.weakNegative / s.strongNegative.toDouble))
  //  res.sortBy(_._1.toEpochDay).foreach(println(_))
  val TIMESTAMP_GRANULARITY_IN_DAYS = args(3).toInt
  val dataSource = args(4)
  val statSampleSize = 1000000
  val evaluationStepDurationInDays:Int = GLOBAL_CONFIG.getEvaluationStepDurationInDays(dataSource)
  val dsName = inputFile.getName.split("\\.")(0)
  val resultFileStats = new File(resultDir.getAbsolutePath + s"/${dsName}_stats.csv")
  val resultFileGraph = new File(resultDir.getAbsolutePath + s"/${dsName}_graphSet.json")
  GLOBAL_CONFIG.setDatesForDataSource(dataSource)
  val tfIDf = tfIDFRootDir.listFiles().toIndexedSeq
    .map(f => (LocalDate.parse(f.getName.split("\\.")(0)), TFIDFMapStorage.fromJsonFile(f.getAbsolutePath)))
    .toMap
  val graph = SlimGraphWithoutWeight.fromJsonFile(inputFile.getAbsolutePath)
  println(graph.verticesOrdered.size)
  println(graph.adjacencyList.size)
  println(graph.adjacencyList.map(_._2.size).sum)
  val aggregator = new MultipleEvenWeightStatCounter(dsName, graph, tfIDf, TIMESTAMP_GRANULARITY_IN_DAYS, resultFileStats, resultFileGraph)
  val counts = aggregator.aggregateEventCounts(evaluationStepDurationInDays,statSampleSize)
  counts.values.foreach(c => {
    val dir = new File(resultDir.getAbsolutePath + s"/$dsName/")
    dir.mkdir()
    c.toJsonFile(new File(dir.getAbsolutePath + s"/${c.trainTimeEnd}.json"))
  })

}
