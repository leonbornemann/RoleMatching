package de.hpi.tfm.evaluation

import de.hpi.tfm.fact_merging.optimization.{GreedyEdgeWeightOptimizer, TupleMerge}
import de.hpi.tfm.io.IOService

object TupleMergeCliqueSizeHistogramMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val mergesCorrect = TupleMerge.loadCorrectMerges(GreedyEdgeWeightOptimizer.methodName)
    .filter(_.clique.size>1)
  val mergesIncorrect = TupleMerge.loadIncorrectMerges(GreedyEdgeWeightOptimizer.methodName)
    .filter(_.clique.size>1)
  println(mergesCorrect.size)
  println(mergesIncorrect.size)
  val eval = TupleMergeEvaluationResult.loadFromStandardFile(GreedyEdgeWeightOptimizer.methodName)
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
