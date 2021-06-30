package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.evaluation.data.SlimGraphWithoutWeight
import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScoreOccurrenceStats
import de.hpi.tfm.fact_merging.optimization.tuning.StatisticBasedWeightTuningMain.inputFile
import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate

object ScoringMain extends App {
//  val inputFile = new File(args(0))
//  val resultDir = new File(args(1))
//  val tuningRootDir = new File(args(2))
//  resultDir.mkdir()
//  val TIMESTAMP_GRANULARITY_IN_DAYS = args(3).toInt
//  val dataSource = args(4)
//  IOService.setDatesForDataSource(dataSource)
//  val dsName = inputFile.getName.split("\\.")(0)
//  val tunedWeights = new File(tuningRootDir.getAbsolutePath+s"/$dsName/").listFiles().toIndexedSeq
//    .map(f => (LocalDate.parse(f.getName.split("\\.")(0)),MultipleEventWeightScoreOccurrenceStats.fromJsonFile(f.getAbsolutePath)))
//    .toMap
//  val graph = SlimGraphWithoutWeight.fromJsonFile(inputFile.getAbsolutePath)
//  val scorer = new SlimGraphWithoutWeightScorer(dsName,graph, TIMESTAMP_GRANULARITY_IN_DAYS:Int,tunedWeights)
}
