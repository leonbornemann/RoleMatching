package de.hpi.role_matching.evaluation.blocking.ground_truth

import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object LabelledRoleMatchingEvaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputLabelDirs = new File(args(0)).listFiles()
  val rolesetFiles = new File(args(1)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultPR = new PrintWriter(args(2))
  val RoleMatchEvaluator = new RoleMatchEvaluator(rolesetFiles)
  //RoleMatchEvaluator.executeLabelGrouping(inputLabelDirs)
  RoleMatchEvaluator.executeEvaluation(inputLabelDirs, resultPR)
}
