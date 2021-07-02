package de.hpi.role_matching.evaluation.edge

import de.hpi.socrata.io.Socrata_IOService
import de.hpi.role_matching.compatibility.GraphConfig

import java.io.File
import java.time.LocalDate

object EdgeBasedEvaluationWikipediaStyleMain extends App {
  Socrata_IOService.socrataDir = args(0)
  val subdomain = args(1)
  val minEvidence = args(2).toInt
  val timeRangeStart = LocalDate.parse(args(3))
  val timeRangeEnd = LocalDate.parse(args(4))
  val graphConfig = GraphConfig(minEvidence, timeRangeStart, timeRangeEnd)
  val minEvidenceEval = args(5).toInt
  val timeRangeStartEval = LocalDate.parse(args(6))
  val timeRangeEndEval = LocalDate.parse(args(7))
  val resultFileJson = new File(args(8))
  val resultFileStats = new File(args(9))
  val graphConfigEval = GraphConfig(minEvidenceEval, timeRangeStartEval, timeRangeEndEval)
  val edgeBasedEvaluator = new WikipediaEdgeBasedEvaluator(subdomain, graphConfig, graphConfigEval, resultFileJson, resultFileStats)
  edgeBasedEvaluator.evaluate()
}
