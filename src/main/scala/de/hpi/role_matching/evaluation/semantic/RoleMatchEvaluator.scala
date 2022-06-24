package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.{SimpleCompatbilityGraphEdge, SimpleCompatbilityGraphEdgeID}
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source

class RoleMatchEvaluator(rolesetFilesNoneDecayed: Array[File]) {

  def executeForSimpleEdgeFile(inputEdgeFiles: Array[File],
                               resultPR: PrintWriter,
                               resultPRDecay: PrintWriter,
                               decayThreshold:Double
                              ) = {
    DECAY_THRESHOLD = decayThreshold
    RoleMatchStatistics.appendSchema(resultPR)
    val decayEvaluator = new DecayEvaluator(resultPRDecay);
    inputEdgeFiles.foreach(f => {
      val dataset = f.getName.split("\\.")(0)
      val rolesetNoDecay = Roleset.fromJsonFile(rolesetFilesNoneDecayed.find(f => f.getName.contains(dataset)).get.getAbsolutePath)
      val stringToLineageMap = rolesetNoDecay.getStringToLineageMap
      val groundTruthExamples = SimpleCompatbilityGraphEdgeID.iterableFromJsonObjectPerLineFile(f.getAbsolutePath)
        .map(e => (SimpleCompatbilityGraphEdge(stringToLineageMap(e.v1),stringToLineageMap(e.v2)),false))
      val retainedEdges = serializeSample(dataset,groundTruthExamples,None,resultPR)
      if(runDecayEvaluation) {
        decayEvaluator.addRecords(dataset, retainedEdges, trainTimeEnd)
      }
    resultPRDecay.close()
    resultPR.close()
    })
  }


  val trainTimeEnd = LocalDate.parse("2016-05-07")
  var DECAY_THRESHOLD = 0.94
  val runDecayEvaluation = true

  def getEdgeFromFile(rolesetNoDecay:Roleset,s: String) = {
    val tokens = s.split(",")
    val firstID = tokens(0).toInt
    val secondID = tokens(1).toInt
    val isTrueMatch = tokens(2).toBoolean
    val rl1 = rolesetNoDecay.positionToRoleLineage(firstID)
    val rl2 = rolesetNoDecay.positionToRoleLineage(secondID)
    (SimpleCompatbilityGraphEdge(rl1,rl2),isTrueMatch)
  }

  def getSamplingGroup(decayedCompatibilityPercentage: Double) = if(decayedCompatibilityPercentage<0.7) "[0.0,0.7)" else if(decayedCompatibilityPercentage < 1.0) "[0.7,1.0)" else "1.0"

  def getDecayedEdgeFromUndecayedEdge(edgeNoDecay: SimpleCompatbilityGraphEdge,beta:Double):SimpleCompatbilityGraphEdge = {
    val rl1WithDecay = edgeNoDecay.v1.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    val rl2WithDecay = edgeNoDecay.v2.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    SimpleCompatbilityGraphEdge(RoleLineageWithID(edgeNoDecay.v1.id,rl1WithDecay.toSerializationHelper),RoleLineageWithID(edgeNoDecay.v2.id,rl2WithDecay.toSerializationHelper))
  }

  def executeEvaluation(inputLabelDirs: Array[File], resultPR: PrintWriter, resultPRDecay: PrintWriter) = {
    val counts = collection.mutable.HashMap[String,collection.mutable.HashMap[String,Int]]()
    inputLabelDirs.map(_.getName).foreach(n => counts.put(n,collection.mutable.HashMap[String,Int]()))
    RoleMatchStatistics.appendSchema(resultPR)
    val decayEvaluator = new DecayEvaluator(resultPRDecay);
    inputLabelDirs.foreach{case (inputLabelDir) => {
      val dataset = inputLabelDir.getName
      val rolesetNoDecay = Roleset.fromJsonFile(rolesetFilesNoneDecayed.find(f => f.getName.contains(inputLabelDir.getName)).get.getAbsolutePath)
      val groundTruthExamples = inputLabelDir.listFiles().flatMap(f => Source.fromFile(f).getLines().toIndexedSeq.tail)
        .map(s => getEdgeFromFile(rolesetNoDecay, s))
      val retainedEdges = serializeSample(dataset,groundTruthExamples.iterator,Some(counts),resultPR)
      if(runDecayEvaluation) {
        decayEvaluator.addRecords(dataset, retainedEdges, trainTimeEnd)
      }
    }}
    resultPRDecay.close()
    resultPR.close()
    counts.foreach{case (ds,map) => println(ds);map.foreach(println)}
  }

  def serializeSample(dataset:String,
                      groundTruthExamples: Iterator[(SimpleCompatbilityGraphEdge, Boolean)],
                      counts:Option[collection.mutable.HashMap[String,collection.mutable.HashMap[String,Int]]],
                      resultPR:PrintWriter) = {
    val retainedSamples = collection.mutable.ArrayBuffer[(SimpleCompatbilityGraphEdge, Boolean)]()
    var countBelow80 = 0
    var countBelow100 = 0
    var count100 = 0
    groundTruthExamples
      //.filter(_._2)
      .foreach{case (edgeNoDecay,label)=> {
        val roleMatchStatistics = new RoleMatchStatistics(dataset,edgeNoDecay,label,DECAY_THRESHOLD,trainTimeEnd)
        val map = counts.map(_.getOrElseUpdate(dataset,collection.mutable.HashMap[String,Int]()))
        if(roleMatchStatistics.nonDecayCompatibilityPercentage < 0.7){
          if(countBelow80<100 || map.isEmpty){
            if(map.isDefined){
              val curCount = map.get.getOrElse(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage),0)
              map.get.put(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage),curCount+1)
            }
            countBelow80+=1
            retainedSamples.append((edgeNoDecay,label))
            roleMatchStatistics.appendStatRow(resultPR)
          } else {
            println(s"Skipping record in $dataset")
          }
        } else if(roleMatchStatistics.nonDecayCompatibilityPercentage<1.0){
          if(countBelow100<100 || map.isEmpty){
            if(map.isDefined){
              val curCount = map.get.getOrElse(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage),0)
              map.get.put(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage),curCount+1)
            }
            countBelow100+=1
            retainedSamples.append((edgeNoDecay,label))
            roleMatchStatistics.appendStatRow(resultPR)
          } else {
            println(s"Skipping record in $dataset")
          }
        } else {
          if(count100<100 || map.isEmpty){
            if(map.isDefined) {
              val curCount = map.get.getOrElse(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage), 0)
              map.get.put(getSamplingGroup(roleMatchStatistics.nonDecayCompatibilityPercentage), curCount + 1)
            }
            count100+=1
            retainedSamples.append((edgeNoDecay,label))
            roleMatchStatistics.appendStatRow(resultPR)
          } else {
            println(s"Skipping record in $dataset")
          }
        }
      }}
    println(countBelow80,countBelow100,count100)
    retainedSamples
  }

}
