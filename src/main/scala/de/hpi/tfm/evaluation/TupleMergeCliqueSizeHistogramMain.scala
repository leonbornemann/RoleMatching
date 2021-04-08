package de.hpi.tfm.evaluation

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.evaluation.EdgeBasedEvaluationMain.args
import de.hpi.tfm.fact_merging.optimization.{GreedyEdgeWeightOptimizer, TupleMerge}
import de.hpi.tfm.io.IOService

import java.time.LocalDate

object TupleMergeCliqueSizeHistogramMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val minEvidence = args(2).toInt
  val timeRangeStart = LocalDate.parse(args(3))
  val timeRangeEnd = LocalDate.parse(args(4))
  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
  val mergesCorrect = TupleMerge.loadCorrectMerges(subdomain,GreedyEdgeWeightOptimizer.methodName,graphConfig)
    .filter(_.clique.size>1)
  val mergesIncorrect = TupleMerge.loadIncorrectMerges(subdomain,GreedyEdgeWeightOptimizer.methodName,graphConfig)
    .filter(_.clique.size>1)
  println(mergesCorrect.size)
  println(mergesIncorrect.size)
  val eval = TupleMergeEvaluationResult.loadFromStandardFile(subdomain,GreedyEdgeWeightOptimizer.methodName,graphConfig)
  eval.printStats()
  private val allMerges = mergesCorrect ++ mergesIncorrect
  val histAll = Histogram(allMerges.map(_.clique.size),true)
  val histCorrect = Histogram(mergesCorrect.map(_.clique.size),true)
  val histIncorrect = Histogram(mergesIncorrect.map(_.clique.size),true)
  println("correct")
  histCorrect.printAll()
  println("incorrect")
  histIncorrect.printAll()
  println("all")
  histAll.printAll()
  println("Total reduction in facts:")
  println(allMerges.map(_.clique.size -1).sum)
}
