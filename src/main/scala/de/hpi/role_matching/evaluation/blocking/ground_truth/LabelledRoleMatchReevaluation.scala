package de.hpi.role_matching.evaluation.blocking.ground_truth

import de.hpi.util.GLOBAL_CONFIG

import java.io.File

object LabelledRoleMatchReevaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputEdgeFiles = new File(args(0)).listFiles()
  val rolesetFilesNoneDecayed = new File(args(1)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultDir = new File(args(2))
  val targetMissingDataShare = if(args.size == 4) Some(args(3).toDouble) else None
  val RoleMatchEvaluator = new RoleMatchEvaluator(rolesetFilesNoneDecayed)
  RoleMatchEvaluator.reexecuteForStatCSVFile(inputEdgeFiles, resultDir, 0.57)
}
