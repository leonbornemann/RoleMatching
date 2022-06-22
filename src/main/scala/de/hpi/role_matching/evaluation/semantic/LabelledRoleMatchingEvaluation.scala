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
  val rolesetFilesDecayed = new File(args(1)).listFiles()
  val rolesetFilesNoneDecayed = new File(args(2)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultPR = new PrintWriter(args(3))
  val resultPRDecay = new PrintWriter(args(4))
  val trainTimeEnd = LocalDate.parse("2016-05-07")
  var counts = collection.mutable.HashMap[String,collection.mutable.HashMap[String,Int]]()
  val runDecayEvaluation = true
  inputLabelDirs.map(_.getName).foreach(n => counts.put(n,collection.mutable.HashMap[String,Int]()))

  val DECAY_THRESHOLD = 0.94

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

  RoleMatchStatistics.appendSchema(resultPR)

  def getDecayedEdgeFromUndecayedEdge(edgeNoDecay: SimpleCompatbilityGraphEdge,beta:Double):SimpleCompatbilityGraphEdge = {
    val rl1WithDecay = edgeNoDecay.v1.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    val rl2WithDecay = edgeNoDecay.v2.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    SimpleCompatbilityGraphEdge(RoleLineageWithID(edgeNoDecay.v1.id,rl1WithDecay.toSerializationHelper),RoleLineageWithID(edgeNoDecay.v2.id,rl2WithDecay.toSerializationHelper))
  }

  //tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t))
  def serializeSample(dataset:String, groundTruthExamples: Array[(SimpleCompatbilityGraphEdge, SimpleCompatbilityGraphEdge, Boolean)]) = {
    val retainedSamples = collection.mutable.ArrayBuffer[(SimpleCompatbilityGraphEdge, SimpleCompatbilityGraphEdge, Boolean)]()
    var countBelow80 = 0
    var countBelow100 = 0
    var count100 = 0
    val roles = groundTruthExamples
      .flatMap(e => Set(e._2.v1.roleLineage.toRoleLineage,e._1.v1.roleLineage.toRoleLineage))
      .toSet
    val tupleToNonWcTransitions = roles
      .map(r => (r,r.informativeValueTransitions))
      .toMap
    groundTruthExamples
      //.filter(_._2)
      .foreach{case (edgeDecayOld,edgeNoDecay,label)=> {
        val roleMatchStatistics = new RoleMatchStatistics(dataset,edgeNoDecay,label,DECAY_THRESHOLD,trainTimeEnd)
        val map = counts.getOrElseUpdate(dataset,collection.mutable.HashMap[String,Int]())
        if(roleMatchStatistics.nonDecayCompatibilityPercentage < 0.7){
          if(countBelow80<100){
            val curCount = map.getOrElse(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage),0)
            map.put(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage),curCount+1)
            countBelow80+=1
            retainedSamples.append((edgeDecayOld,edgeNoDecay,label))
            roleMatchStatistics.appendStatRow(resultPR)
          } else {
            println(s"Skipping record in $dataset")
          }
        } else if(roleMatchStatistics.nonDecayCompatibilityPercentage<1.0){
          if(countBelow100<100){
            val curCount = map.getOrElse(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage),0)
            map.put(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage),curCount+1)
            countBelow100+=1
            retainedSamples.append((edgeDecayOld,edgeNoDecay,label))
            roleMatchStatistics.appendStatRow(resultPR)
          } else {
            println(s"Skipping record in $dataset")
          }
        } else {
          if(count100<100){
            val curCount = map.getOrElse(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage),0)
            map.put(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage),curCount+1)
            count100+=1
            retainedSamples.append((edgeDecayOld,edgeNoDecay,label))
            roleMatchStatistics.appendStatRow(resultPR)
          } else {
            println(s"Skipping record in $dataset")
          }
        }
      }}
    println(countBelow80,countBelow100,count100)
    retainedSamples
  }

  val decayEvaluator = new DecayEvaluator(resultPRDecay);
  inputLabelDirs.foreach{case (inputLabelDir) => {
    val dataset = inputLabelDir.getName
    val roleset = Roleset.fromJsonFile(rolesetFilesDecayed.find(f => f.getName.contains(inputLabelDir.getName)).get.getAbsolutePath)
    val rolesetNoDecay = Roleset.fromJsonFile(rolesetFilesNoneDecayed.find(f => f.getName.contains(inputLabelDir.getName)).get.getAbsolutePath)
    val stringToLineageMapNoDecay = rolesetNoDecay.getStringToLineageMap
    val groundTruthExamples = inputLabelDir.listFiles().flatMap(f => Source.fromFile(f).getLines().toIndexedSeq.tail)
      .map(s => getEdgeFromFile(roleset, stringToLineageMapNoDecay, s))
    val retainedEdges = serializeSample(dataset,groundTruthExamples)
    if(runDecayEvaluation) {
      decayEvaluator.addRecords(dataset, retainedEdges, trainTimeEnd)
    }
  }}
  resultPRDecay.close()
  resultPR.close()


  def getSamplingGroup(decayedCompatibilityPercentage: Double) = if(decayedCompatibilityPercentage<0.7) "[0.0,0.7)" else if(decayedCompatibilityPercentage < 1.0) "[0.7,1.0)" else "1.0"

  counts.foreach{case (ds,map) => println(ds);map.foreach(println)}


}
