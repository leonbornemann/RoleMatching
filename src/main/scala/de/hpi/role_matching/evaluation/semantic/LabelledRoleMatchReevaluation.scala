package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object LabelledRoleMatchReevaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputEdgeFiles = new File(args(0)).listFiles()
  val rolesetFilesNoneDecayed = new File(args(1)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultPR = new PrintWriter(args(2))
  val resultPRDecay = new PrintWriter(args(3))
  val RoleMatchEvaluator = new RoleMatchEvaluator(rolesetFilesNoneDecayed)
  RoleMatchEvaluator.executeForSimpleEdgeFile(inputEdgeFiles,resultPR,resultPRDecay,0.57)
}
