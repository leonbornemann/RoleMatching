package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.wikipedia.infobox.statistics.edge.EdgeAnalyser
import de.hpi.tfm.evaluation.data.GeneralEdge
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.IOService

import java.io.{File, PrintWriter}
import java.time.LocalDate

object BrokenEdgeAnalysis extends App {
  val edgeFile = args(0)
  val trainTimeEnd = LocalDate.parse("2020-04-30")
  val edges = GeneralEdge.fromJsonObjectPerLineFile(edgeFile)
  val trainGraphConfig = GraphConfig(0,IOService.STANDARD_TIME_FRAME_START,trainTimeEnd)
//  val invalidEdges = edges.filter(e => {
//    val row = e.toGeneralEdgeStatRow(1, trainGraphConfig, Set(), Map(), 0)
//    row.remainsValid && row.isInteresting && row.trainMetrics(0) < 0.1
//  })
//  println()
  val edgesFiltered = edges.filter(e => e.v1.isNumeric || e.v2.isNumeric)
  println(edges.size)
  println(edgesFiltered.size)
  val analyser = new EdgeAnalyser(edges,trainGraphConfig,1,GLOBAL_CONFIG.nonInformativeValues,None)
  val a = analyser.transitionHistogramForTFIDF
  analyser.toCsvFile(new File("test.csv"))
  println()

}
