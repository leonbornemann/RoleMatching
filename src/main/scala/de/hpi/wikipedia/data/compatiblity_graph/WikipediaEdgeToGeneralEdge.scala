package de.hpi.wikipedia.data.compatiblity_graph

import java.io.PrintWriter

object WikipediaEdgeToGeneralEdge extends App {
  val inputFile = args(0)
  val outputFile = args(1)
  val wikipediaEdges = WikipediaInfoboxValueHistoryMatch.fromJsonObjectPerLineFile(inputFile)
  val generalEdges = wikipediaEdges.map(_.toGeneralEdge)
  val pr = new PrintWriter(outputFile)
  generalEdges.foreach(_.appendToWriter(pr, false, true))
  pr.close()

}
