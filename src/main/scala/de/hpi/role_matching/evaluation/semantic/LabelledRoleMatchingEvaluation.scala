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

  val DECAY_THRESHOLD = 0.7

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

  resultPR.println("dataset,id1,id2,isInStrictBlockingDecay,isInStrictBlockingNoDecay,isInValueSetBlocking,isInSequenceBlocking," +
    "isInExactMatchBlocking,isSemanticRoleMatch,compatibilityPercentageDecay,compatibilityPercentageNoDecay,exactSequenceMatchPercentage," +
    "hasTransitionOverlapNoDecay,hasTransitionOverlapDecay,hasValueSetOverlap,isInSVABlockingNoDecay,isInSVABlockingDecay")

  def getDecayedEdgeFromUndecayedEdge(edgeNoDecay: SimpleCompatbilityGraphEdge,beta:Double):SimpleCompatbilityGraphEdge = {
    val rl1WithDecay = edgeNoDecay.v1.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    val rl2WithDecay = edgeNoDecay.v2.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    SimpleCompatbilityGraphEdge(RoleLineageWithID(edgeNoDecay.v1.id,rl1WithDecay.toSerializationHelper),RoleLineageWithID(edgeNoDecay.v2.id,rl2WithDecay.toSerializationHelper))
  }

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
        val edgeDecay = getDecayedEdgeFromUndecayedEdge(edgeNoDecay,DECAY_THRESHOLD)
        val id1 = edgeDecay.v1.csvSafeID
        val id2 = edgeDecay.v2.csvSafeID
        val rl1 = edgeDecay.v1.roleLineage.toRoleLineage
        val rl2 = edgeDecay.v2.roleLineage.toRoleLineage
        val rl1Projected = rl1.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
        val rl2Projected = rl2.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
        val isInValueSetBlocking = rl1Projected.nonWildcardValueSetBefore(trainTimeEnd) == rl2Projected.nonWildcardValueSetBefore(trainTimeEnd)
        val isInSequenceBlocking = rl1Projected.nonWildcardValueSequenceBefore(trainTimeEnd) == rl2Projected.nonWildcardValueSequenceBefore(trainTimeEnd)
        val isInExactSequenceBlocking = rl1Projected.exactlyMatchesWithoutDecay(rl2Projected,trainTimeEnd)
        val hasTransitionOverlapDecay = rl1Projected.informativeValueTransitions.intersect(rl2Projected.informativeValueTransitions).size>=1
        val statRow = new BasicStatRow(rl1Projected,rl2Projected,trainTimeEnd)
        val hasValueSetOverlap = rl1Projected.nonWildcardValueSetBefore(trainTimeEnd.plusDays(1)).exists(v => rl2Projected.nonWildcardValueSetBefore(trainTimeEnd.plusDays(1)).contains(v))
        val isInSVABlockingDecay = GLOBAL_CONFIG.STANDARD_TIME_RANGE
          .filter(!_.isAfter(trainTimeEnd))
          .exists(t => rl1Projected.valueAt(t) == rl2Projected.valueAt(t) && !RoleLineage.isWildcard(rl1Projected.valueAt(t)))
        //no decay:
        val rl1NoDecay = edgeNoDecay.v1.roleLineage.toRoleLineage
        val rl2NoDecay = edgeNoDecay.v2.roleLineage.toRoleLineage
        val rl1ProjectedNoDecay = rl1NoDecay.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
        val rl2ProjectedNoDecay = rl2NoDecay.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
        val hasTransitionOverlapNoDecay = rl1ProjectedNoDecay.informativeValueTransitions.intersect(rl2ProjectedNoDecay.informativeValueTransitions).size>=1
        val statRowNoDecay = new BasicStatRow(rl1ProjectedNoDecay,rl2ProjectedNoDecay,trainTimeEnd)
        val isInSVABlockingNoDecay = GLOBAL_CONFIG.STANDARD_TIME_RANGE
          .filter(!_.isAfter(trainTimeEnd))
          .exists(t => rl1ProjectedNoDecay.valueAt(t) == rl2ProjectedNoDecay.valueAt(t) && !RoleLineage.isWildcard(rl1ProjectedNoDecay.valueAt(t)))
        //csv fields:
        val isInStrictBlocking = statRow.remainsValidFullTimeSpan
        val isInStrictBlockingNoDecay = statRowNoDecay.remainsValidFullTimeSpan
        val map = counts.getOrElseUpdate(dataset,collection.mutable.HashMap[String,Int]())
        val decayedCompatibilityPercentage = rl1Projected.getCompatibilityTimePercentage(rl2Projected, trainTimeEnd)
        val nonDecayCompatibilityPercentage = rl1ProjectedNoDecay.getCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)
        val exactSequenceMatchPercentage = rl1ProjectedNoDecay.exactMatchesWithoutWildcardPercentage(rl2ProjectedNoDecay,trainTimeEnd)
        if(nonDecayCompatibilityPercentage < 0.7){
          if(countBelow80<100){
            val curCount = map.getOrElse(getSamplingGroup(nonDecayCompatibilityPercentage),0)
            map.put(getSamplingGroup(nonDecayCompatibilityPercentage),curCount+1)
            countBelow80+=1
            retainedSamples.append((edgeDecayOld,edgeNoDecay,label))
            resultPR.println(s"$dataset,$id1,$id2,$isInStrictBlocking,$isInStrictBlockingNoDecay,$isInValueSetBlocking,$isInSequenceBlocking,$isInExactSequenceBlocking,$label," +
              s"$decayedCompatibilityPercentage," +
              s"${rl1ProjectedNoDecay.getCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)}," +
              s"$exactSequenceMatchPercentage,$hasTransitionOverlapNoDecay,$hasTransitionOverlapDecay,$hasValueSetOverlap,$isInSVABlockingNoDecay,$isInSVABlockingDecay")
          } else {
            println(s"Skipping record in $dataset")
          }
        } else if(nonDecayCompatibilityPercentage<1.0){
          if(countBelow100<100){
            val curCount = map.getOrElse(getSamplingGroup(nonDecayCompatibilityPercentage),0)
            map.put(getSamplingGroup(nonDecayCompatibilityPercentage),curCount+1)
            countBelow100+=1
            retainedSamples.append((edgeDecayOld,edgeNoDecay,label))
            resultPR.println(s"$dataset,$id1,$id2,$isInStrictBlocking,$isInStrictBlockingNoDecay,$isInValueSetBlocking,$isInSequenceBlocking,$isInExactSequenceBlocking,$label," +
              s"$decayedCompatibilityPercentage," +
              s"${rl1ProjectedNoDecay.getCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)}," +
              s"$exactSequenceMatchPercentage,$hasTransitionOverlapNoDecay,$hasTransitionOverlapDecay,$hasValueSetOverlap,$isInSVABlockingNoDecay,$isInSVABlockingDecay")
          } else {
            println(s"Skipping record in $dataset")
          }
        } else {
          if(count100<100){
            val curCount = map.getOrElse(getSamplingGroup(nonDecayCompatibilityPercentage),0)
            map.put(getSamplingGroup(nonDecayCompatibilityPercentage),curCount+1)
            count100+=1
            retainedSamples.append((edgeDecayOld,edgeNoDecay,label))
            resultPR.println(s"$dataset,$id1,$id2,$isInStrictBlocking,$isInStrictBlockingNoDecay,$isInValueSetBlocking,$isInSequenceBlocking,$isInExactSequenceBlocking,$label," +
              s"$decayedCompatibilityPercentage," +
              s"${rl1ProjectedNoDecay.getCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)}," +
              s"$exactSequenceMatchPercentage,$hasTransitionOverlapNoDecay,$hasTransitionOverlapDecay,$hasValueSetOverlap,$isInSVABlockingNoDecay,$isInSVABlockingDecay")
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
