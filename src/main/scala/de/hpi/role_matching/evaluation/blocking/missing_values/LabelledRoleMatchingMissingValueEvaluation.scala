package de.hpi.role_matching.evaluation.blocking.missing_values

import de.hpi.role_matching.evaluation.blocking.ground_truth.RoleMatchEvaluator
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object LabelledRoleMatchingMissingValueEvaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputLabelDirs = new File(args(0)).listFiles()
  val rolesetRootDir = new File(args(1))
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultDir = new File(args(2))
  val targetBasename = new File(args(0)).getName
  rolesetRootDir.listFiles.foreach(d => {
    println(d)
    println(targetBasename)
    val pr = new PrintWriter(resultDir.getAbsolutePath + s"/${targetBasename}_${d.getName}.csv")
    val rolesetFiles = d.listFiles()
    val RoleMatchEvaluator = new RoleMatchEvaluator(rolesetFiles)
    RoleMatchEvaluator.executeEvaluation(inputLabelDirs, pr)
  })
}
