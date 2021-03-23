package de.hpi.dataset_versioning.db_synthesis.evaluation

import de.hpi.dataset_versioning.db_synthesis.optimization.{GreedyEdgeWeightOptimizer, TupleMerge}
import de.hpi.dataset_versioning.io.IOService

object TupleMergeCliqueSizeHistogramMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val mergesCorrect = TupleMerge.loadCorrectMerges(GreedyEdgeWeightOptimizer.methodName)
  val mergesIncorrect = TupleMerge.loadIncorrectMerges(GreedyEdgeWeightOptimizer.methodName)

  val histAll = Histogram((mergesCorrect ++ mergesIncorrect).map(_.clique.size))
  val histCorrect = Histogram(mergesCorrect.map(_.clique.size))
  val histIncorrect = Histogram(mergesIncorrect.map(_.clique.size))
  println("correct")
  histCorrect.printAll()
  println("incorrect")
  histIncorrect.printAll()
  println("all")
  histAll.printAll()
}
