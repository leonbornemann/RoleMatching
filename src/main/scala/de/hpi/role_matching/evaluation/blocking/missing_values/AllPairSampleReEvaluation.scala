package de.hpi.role_matching.evaluation.blocking.missing_values

import de.hpi.role_matching.data.RoleMatchCandidateIds
import de.hpi.role_matching.evaluation.blocking.ground_truth.{DatasetAndIDJson, RoleMatchEvaluator}
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object AllPairSampleReEvaluation extends App {
  println(s"called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputFileDir = args(0)
  val rolesetFile = new File(args(1))
  val missingDataDegree = rolesetFile.getParentFile.getName
  val resultDir = args(2) + s"/$missingDataDegree/"
  println(rolesetFile)
  new File(resultDir).mkdirs()
  val dataset = rolesetFile.getName.split("\\.")(0)
  private val datasetFileName = rolesetFile.getName
  private val resultFile = resultDir + s"/allPairs_$datasetFileName.csv"
  val pr = new PrintWriter(resultFile)
  val inputFile = inputFileDir + s"/$datasetFileName"
  println(s"Running with roleset ${rolesetFile.getAbsolutePath} for sample $inputFile to destination $resultFile")
  val RoleMatchEvaluator = new RoleMatchEvaluator(Array(rolesetFile))
  val it = RoleMatchCandidateIds
    .iterableFromJsonObjectPerLineFile(inputFile)
    .map(c => DatasetAndIDJson(dataset,c.v1,c.v2))
  RoleMatchEvaluator.executeEvaluationForIterator(it, pr)


}
