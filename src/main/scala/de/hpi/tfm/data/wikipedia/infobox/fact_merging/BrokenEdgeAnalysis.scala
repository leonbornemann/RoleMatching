package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.evaluation.data.GeneralEdge
import de.hpi.tfm.io.IOService

import java.time.LocalDate

object BrokenEdgeAnalysis extends App {
  val edgeFile = args(0)
  val trainTimeEnd = LocalDate.parse("2020-04-30")
  val edges = GeneralEdge.fromJsonObjectPerLineFile(edgeFile)
  val trainGraphConfig = GraphConfig(0,IOService.STANDARD_TIME_FRAME_START,trainTimeEnd)
  val invalidEdges = edges.filter(e => !e.toGeneralEdgeStatRow(1,trainGraphConfig).remainsValid && e.toGeneralEdgeStatRow(1,trainGraphConfig).isInteresting)
  println()

}
