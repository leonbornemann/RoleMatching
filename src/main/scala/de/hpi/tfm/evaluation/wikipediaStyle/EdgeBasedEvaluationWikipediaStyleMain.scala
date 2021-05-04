package de.hpi.tfm.evaluation.wikipediaStyle

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.evaluation.EdgeBasedEvaluationMain.args
import de.hpi.tfm.evaluation.EdgeBasedEvaluator
import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate

object EdgeBasedEvaluationWikipediaStyleMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val minEvidence = args(2).toInt
  val timeRangeStart = LocalDate.parse(args(3))
  val timeRangeEnd = LocalDate.parse(args(4))
  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
  val minEvidenceEval = args(5).toInt
  val timeRangeStartEval = LocalDate.parse(args(6))
  val timeRangeEndEval = LocalDate.parse(args(7))
  val resultFileJson = new File(args(8))
  val resultFileEdges = new File(args(9))
  val graphConfigEval = GraphConfig(minEvidenceEval,timeRangeStartEval,timeRangeEndEval)
  val edgeBasedEvaluator = new WikipediaEdgeBasedEvaluator(subdomain,graphConfig,graphConfigEval,resultFileJson,resultFileEdges)
  edgeBasedEvaluator.evaluate()
}
