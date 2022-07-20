package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG

import java.io.File

object LabelledRoleMatchReevaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputEdgeFiles = new File(args(0)).listFiles()
  val rolesetFilesNoneDecayed = new File(args(1)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultDir = new File(args(2))
  val RoleMatchEvaluator = new RoleMatchEvaluator(rolesetFilesNoneDecayed)
  RoleMatchEvaluator.reexecuteForStatCSVFile(inputEdgeFiles,resultDir,0.57)
}
