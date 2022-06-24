package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.{RoleLineage, RoleLineageWithID, Roleset}
import de.hpi.role_matching.evaluation.tuning.BasicStatRow

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source

object LabelledRoleMatchingEvaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputLabelDirs = new File(args(0)).listFiles()
  val rolesetFilesNoneDecayed = new File(args(1)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultPR = new PrintWriter(args(2))
  val resultPRDecay = new PrintWriter(args(3))
  val RoleMatchEvaluator = new RoleMatchEvaluator(rolesetFilesNoneDecayed)
  RoleMatchEvaluator.executeEvaluation(inputLabelDirs,resultPR,resultPRDecay)




}
