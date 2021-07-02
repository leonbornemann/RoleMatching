package de.hpi.role_matching.evaluation.edge

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.compatibility.GraphConfig
import de.hpi.role_matching.compatibility.graph.representation.simple.GeneralEdge

import java.io.PrintWriter
import java.time.LocalDate

object DirectEdgeFileEvaluation extends App {
  val edgeFile = args(0)
  val resultFile = args(1)
  val timeRangeStart = LocalDate.parse(args(2))
  val timeRangeEnd = LocalDate.parse(args(3))
  val trainGraphConfig = GraphConfig(0, timeRangeStart, timeRangeEnd)
  val edges = GeneralEdge.fromJsonObjectPerLineFile(edgeFile)
  val resultPR = new PrintWriter(resultFile)
  var i = 0
  val hist = GeneralEdge.getTransitionHistogramForTFIDF(edges, 1)
  val count = GeneralEdge.getLineageCount(edges)
  edges.foreach { case e => {
    val statRow = e.toGeneralEdgeStatRow(1, trainGraphConfig, GLOBAL_CONFIG.nonInformativeValues, hist, count)
    if (i == 0)
      resultPR.println(statRow.getSchema.mkString(","))
    resultPR.println(statRow.toCSVLine)
    i += 1
  }
  }
  resultPR.close()
  //timeRangeStartTrain="2019-11-01"
  //timeRangeEndTrain="2020-04-30"

}
