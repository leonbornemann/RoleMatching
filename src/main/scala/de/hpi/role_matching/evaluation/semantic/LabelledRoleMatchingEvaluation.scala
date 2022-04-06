package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.Roleset
import de.hpi.role_matching.evaluation.tuning.BasicStatRow

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source

object LabelledRoleMatchingEvaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputLabelDirs = new File(args(0)).listFiles()
  val rolesetFiles = new File(args(1)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultPR = new PrintWriter(args(2))
  val trainTimeEnd = LocalDate.parse("2016-05-07")

  def getEdgeFromFile(roleset:Roleset,s: String) = {
    val tokens = s.split(",")
    val firstID = tokens(0).toInt
    val secondID = tokens(1).toInt
    val isTrueMatch = tokens(2).toBoolean
    val rl1 = roleset.positionToRoleLineage(firstID)
    val rl2 = roleset.positionToRoleLineage(secondID)
    (SimpleCompatbilityGraphEdge(rl1,rl2),isTrueMatch)
  }

  resultPR.println("dataset,isInStrictBlocking,isSemanticRoleMatch,compatibilityPercentage")
  inputLabelDirs.foreach{case (inputLabelDir) => {
    val dataset = inputLabelDir.getName
    val roleset = Roleset.fromJsonFile(rolesetFiles.find(f => f.getName.contains(inputLabelDir.getName)).get.getAbsolutePath)
    inputLabelDir.listFiles().flatMap(f => Source.fromFile(f).getLines().toIndexedSeq.tail)
      .map(s => getEdgeFromFile(roleset,s))
      //.filter(_._2)
      .foreach{case (e,label)=> appendToResultPr(dataset,e,label)}
  }}
  resultPR.close()

  def appendToResultPr(dataset:String,e: SimpleCompatbilityGraphEdge, label: Boolean) = {
    val rl1 = e.v1.roleLineage.toRoleLineage
    val rl2 = e.v2.roleLineage.toRoleLineage
    val rl1Projected = rl1.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
    val rl2Projected = rl2.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
    val statRow = new BasicStatRow(rl1Projected,rl2Projected,trainTimeEnd)
    val isInStrictBlocking = statRow.remainsValidFullTimeSpan
    if(isInStrictBlocking)
      println()
    resultPR.println(s"$dataset,$isInStrictBlocking,$label,${rl1Projected.getCompatibilityTimePercentage(rl2Projected,trainTimeEnd)}")
  }


}
