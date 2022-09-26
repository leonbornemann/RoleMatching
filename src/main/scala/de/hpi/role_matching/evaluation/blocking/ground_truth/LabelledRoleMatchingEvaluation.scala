package de.hpi.role_matching.evaluation.blocking.ground_truth

import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object LabelledRoleMatchingEvaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputLabelDirs = new File(args(0)).listFiles()
  val rolesetFiles = new File(args(1)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultPR = new PrintWriter(args(2))
  val additionalMissingData = if(args.size == 4) Some(args(3).toDouble) else None
  val RoleMatchEvaluator = new RoleMatchEvaluator(rolesetFiles,additionalMissingData)
  //RoleMatchEvaluator.executeLabelGrouping(inputLabelDirs)
  RoleMatchEvaluator.executeEvaluation(inputLabelDirs, resultPR,Some(new File("/home/leon/data/dataset_versioning/finalExperiments/semanticAnnotation/dgs_new")))
}
