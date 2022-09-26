package de.hpi.role_matching.evaluation.blocking.ground_truth

import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

//java -ea -Xmx100g -cp TODO de.hpi.role_matching.evaluation.blocking.ground_truth.LabelledRoleMatchingMissingValueEvaluation /san2/data/change-exploration/roleMerging/finalExperiments/ground_truth/dgs_cleaned/
object LabelledRoleMatchingMissingValueEvaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputLabelDirs = new File(args(0)).listFiles()
  val rolesetRootDir = new File(args(1))
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultDir = new File(args(2))
  val targetBasename = new File(args(0)).getName
  rolesetRootDir.listFiles.foreach(d => {
    println(d)
    val pr = new PrintWriter(resultDir + s"/${targetBasename}_$d")
    val rolesetFiles = d.listFiles()
    val RoleMatchEvaluator = new RoleMatchEvaluator(rolesetFiles)
    RoleMatchEvaluator.executeEvaluation(inputLabelDirs,pr)
  })
}
