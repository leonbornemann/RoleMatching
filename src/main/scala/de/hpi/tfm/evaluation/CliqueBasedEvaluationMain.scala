package de.hpi.tfm.evaluation

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate

object CliqueBasedEvaluationMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val optimizationMethodName = args(2)
  val targetFunctionName = args(3)
  val minEvidence = args(4).toInt
  val timeRangeStart = LocalDate.parse(args(5))
  val timeRangeEnd = LocalDate.parse(args(6))
  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
  val minEvidenceEval = args(7).toInt
  val timeRangeStartEval = LocalDate.parse(args(8))
  val timeRangeEndEval = LocalDate.parse(args(9))
  val graphConfigEval = GraphConfig(minEvidenceEval,timeRangeStartEval,timeRangeEndEval)
  new CliqueBasedEvaluator(subdomain,optimizationMethodName,targetFunctionName,graphConfig,graphConfigEval)
    .evaluate()
}
