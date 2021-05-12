package de.hpi.tfm.evaluation.data

import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.{RuzickaSimilarity, TransitionHistogramMode}
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.TransitionHistogramMode.TransitionHistogramMode

import java.io.File

object GraphOptimizationMain extends App {
  val edges:IndexedSeq[GeneralEdge] = ??? //get these somewhere!
  val TIMESTAMP_RESOLUTION_IN_DAYS:Long=7
  val histogramMode: TransitionHistogramMode = TransitionHistogramMode.COUNT_NON_CHANGE_ONLY_ONCE //get these as params!
  val slimGraph = SlimOptimizationGraph.fromIdentifiedEdges(edges,new RuzickaSimilarity(TIMESTAMP_RESOLUTION_IN_DAYS,histogramMode)) //or load this directly
  val resultFile = new File("TODO") // get real path somewhere!
  val optimizer = new GreedyEdgeBasedOptimizer(slimGraph.transformToOptimizationGraph,resultFile,0.0) //TODO: get threshold somewere!
  optimizer.runComponentWiseOptimization()
}
