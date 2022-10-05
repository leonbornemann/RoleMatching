package de.hpi.role_matching.evaluation.blocking.missing_values

import de.hpi.role_matching.evaluation.blocking.ground_truth.{DatasetAndIDJson, RoleMatchEvaluator}
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object AllPairSampleReEvaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputFile = args(0)
  val rolesetRootDir = new File(args(1))
  val resultDir = args(2)
  rolesetRootDir.listFiles.foreach(d => {
    println(d)
    val pr = new PrintWriter(resultDir + s"/allPairs_${d.getName}.csv")
    val rolesetFiles = d.listFiles()
    val RoleMatchEvaluator = new RoleMatchEvaluator(rolesetFiles)
    val it = DatasetAndIDJson
      .iterableFromJsonObjectPerLineFile(inputFile)
    RoleMatchEvaluator.executeEvaluationForIterator(it, pr)
  })

}
