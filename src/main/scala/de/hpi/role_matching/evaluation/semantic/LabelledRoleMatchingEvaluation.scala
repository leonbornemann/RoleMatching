package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.Roleset
import de.hpi.role_matching.evaluation.tuning.BasicStatRow

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source

object LabelledRoleMatchingEvaluation extends App {

  val inputLabelDir = args(0)
  val rolesetFile = args(1)
  val roleset = Roleset.fromJsonFile(rolesetFile)
  val resultPR = new PrintWriter(args(2))
  val trainTimeEnd = LocalDate.parse("2016-05-07")

  def getEdgeFromFile(s: String) = {
    val tokens = s.split(",")
    val firstID = tokens(0).toInt
    val secondID = tokens(1).toInt
    val isTrueMatch = tokens(2).toBoolean
    val rl1 = roleset.positionToRoleLineage(firstID)
    val rl2 = roleset.positionToRoleLineage(secondID)
    (SimpleCompatbilityGraphEdge(rl1,rl2),isTrueMatch)
  }


  def appendToResultPr(e: SimpleCompatbilityGraphEdge, label: Boolean) = {
    val statRow = new BasicStatRow(e.v1.roleLineage.toRoleLineage,e.v2.roleLineage.toRoleLineage,trainTimeEnd)
    val isInStrictBlocking = statRow.remainsValidFullTimeSpan
    resultPR.println(s"$isInStrictBlocking,$label")
  }

  resultPR.println("isInStrictBlocking,isSemanticRoleMatch")
  new File(inputLabelDir).listFiles().flatMap(f => Source.fromFile(f).getLines().toIndexedSeq.tail)
    .map(s => getEdgeFromFile(s))
    //.filter(_._2)
    .foreach{case (e,label)=> appendToResultPr(e,label)}
  resultPR.close()
}
