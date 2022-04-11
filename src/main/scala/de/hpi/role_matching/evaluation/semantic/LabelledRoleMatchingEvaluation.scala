package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}
import de.hpi.role_matching.evaluation.tuning.BasicStatRow

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source

object LabelledRoleMatchingEvaluation extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputLabelDirs = new File(args(0)).listFiles()
  val rolesetFilesDecayed = new File(args(1)).listFiles()
  val rolesetFilesNoneDecayed = new File(args(2)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultPR = new PrintWriter(args(3))
  val trainTimeEnd = LocalDate.parse("2016-05-07")
  var counts = collection.mutable.HashMap[String,collection.mutable.HashMap[String,Int]]()
  inputLabelDirs.map(_.getName).foreach(n => counts.put(n,collection.mutable.HashMap[String,Int]()))

  def getEdgeFromFile(rolesetDecay:Roleset,stringToLineageMapNoDecay: Map[String, RoleLineageWithID], s: String) = {
    val tokens = s.split(",")
    val firstID = tokens(0).toInt
    val secondID = tokens(1).toInt
    val isTrueMatch = tokens(2).toBoolean
    val rl1 = rolesetDecay.positionToRoleLineage(firstID)
    val rl2 = rolesetDecay.positionToRoleLineage(secondID)
    val rl1NoDecay = stringToLineageMapNoDecay(rl1.id)
    val rl2NoDecay = stringToLineageMapNoDecay(rl2.id)
    (SimpleCompatbilityGraphEdge(rl1,rl2),SimpleCompatbilityGraphEdge(rl1NoDecay,rl2NoDecay),isTrueMatch)
  }

  resultPR.println("dataset,isInStrictBlockingDecay,isInStrictBlockingNoDecay,isSemanticRoleMatch,compatibilityPercentageDecay,compatibilityPercentageNoDecay")
  inputLabelDirs.foreach{case (inputLabelDir) => {
    val dataset = inputLabelDir.getName
    val roleset = Roleset.fromJsonFile(rolesetFilesDecayed.find(f => f.getName.contains(inputLabelDir.getName)).get.getAbsolutePath)
    val rolesetNoDecay = Roleset.fromJsonFile(rolesetFilesNoneDecayed.find(f => f.getName.contains(inputLabelDir.getName)).get.getAbsolutePath)
    val stringToLineageMapNoDecay = rolesetNoDecay.getStringToLineageMap
    val groundTruthExamples = inputLabelDir.listFiles().flatMap(f => Source.fromFile(f).getLines().toIndexedSeq.tail)
      .map(s => getEdgeFromFile(roleset, stringToLineageMapNoDecay, s))
    groundTruthExamples
      //.filter(_._2)
      .foreach{case (eDecay,eNoDecay,label)=> appendToResultPr(dataset,eDecay,eNoDecay,label)}
  }}
  resultPR.close()

  def getSamplingGroup(decayedCompatibilityPercentage: Double) = if(decayedCompatibilityPercentage<0.8) "[0.0,0.8)" else if(decayedCompatibilityPercentage < 1.0) "[0.8,1.0)" else "1.0"

  def appendToResultPr(dataset:String, edgeDecay: SimpleCompatbilityGraphEdge, edgeNoDecay:SimpleCompatbilityGraphEdge, label: Boolean) = {
    //decay
    val rl1 = edgeDecay.v1.roleLineage.toRoleLineage
    val rl2 = edgeDecay.v2.roleLineage.toRoleLineage
    val rl1Projected = rl1.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
    val rl2Projected = rl2.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
    val statRow = new BasicStatRow(rl1Projected,rl2Projected,trainTimeEnd)
    val isInStrictBlocking = statRow.remainsValidFullTimeSpan
    //no decay:
    val rl1NoDecay = edgeNoDecay.v1.roleLineage.toRoleLineage
    val rl2NoDecay = edgeNoDecay.v2.roleLineage.toRoleLineage
    val rl1ProjectedNoDecay = rl1NoDecay.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
    val rl2ProjectedNoDecay = rl2NoDecay.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
    val statRowNoDecay = new BasicStatRow(rl1ProjectedNoDecay,rl2ProjectedNoDecay,trainTimeEnd)
    val isInStrictBlockingNoDecay = statRowNoDecay.remainsValidFullTimeSpan
    val map = counts.getOrElseUpdate(dataset,collection.mutable.HashMap[String,Int]())
    val decayedCompatibilityPercentage = rl1Projected.getCompatibilityTimePercentage(rl2Projected, trainTimeEnd)
    val curCount = map.getOrElse(getSamplingGroup(decayedCompatibilityPercentage),0)
    map.put(getSamplingGroup(decayedCompatibilityPercentage),curCount+1)
    resultPR.println(s"$dataset,$isInStrictBlocking,$isInStrictBlockingNoDecay,$label," +
      s"$decayedCompatibilityPercentage," +
      s"${rl1ProjectedNoDecay.getCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)}")
  }
  counts.foreach{case (ds,map) => println(ds);map.foreach(println)}


}
