package de.hpi.role_matching.evaluation.semantic

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.{SimpleCompatbilityGraphEdge, SimpleCompatbilityGraphEdgeID}
import de.hpi.role_matching.cbrm.data.{RoleLineage, RoleLineageWithID, Roleset}

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source

class RoleMatchEvaluator(rolesetFilesNoneDecayed: Array[File]) extends StrictLogging{

  def reexecuteForStatCSVFile(inputCSVFiles: Array[File],
                              resultDir: File,
                              decayThreshold:Double
                              ) = {
    DECAY_THRESHOLD = decayThreshold
    resultDir.mkdirs()
    inputCSVFiles.foreach(f => {
      logger.debug(s"Processing ${f}")
      val dataset = f.getName.split("\\.")(0)
      val resultPr = new PrintWriter(resultDir + s"/$dataset.csv")
      RoleMatchStatistics.appendSchema(resultPr)
      val rolesetNoDecay = Roleset.fromJsonFile(rolesetFilesNoneDecayed.find(f => f.getName.contains(dataset)).get.getAbsolutePath)
      val stringToLineageMap = rolesetNoDecay.getStringToLineageMap
      val groundTruthExamplIterator = Source.fromFile(f)
        .getLines()
        .map(l => {
          val tokens = l.split(",")
          SimpleCompatbilityGraphEdgeID(tokens(1),tokens(2))
        })
      //skip schema line
      groundTruthExamplIterator.next()
      val groundTruthExamples = groundTruthExamplIterator
        .map(e => (SimpleCompatbilityGraphEdge(stringToLineageMap(e.v1),stringToLineageMap(e.v2)),false))
      serializeSample(dataset,groundTruthExamples,None,resultPr)
      resultPr.close()
    })
  }


  val trainTimeEnd = LocalDate.parse("2016-05-07")
  var DECAY_THRESHOLD = 0.57
  var DECAY_THRESHOLD_SCB = 0.50
  val runDecayEvaluation = false

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

  def executeLabelGrouping (inputLabelDirs: Array[File]) = {
    inputLabelDirs.foreach{case (inputLabelDir) => {
      val dataset = inputLabelDir.getName
      println(dataset)
      val rolesetNoDecay = Roleset.fromJsonFile(rolesetFilesNoneDecayed.find(f => f.getName.contains(inputLabelDir.getName)).get.getAbsolutePath)
      val groundTruthExamples = inputLabelDir
        .listFiles()
        .flatMap(f => {
          Source
            .fromFile(f)
            .getLines()
            .toIndexedSeq
            .tail
            .map(s => getEdgeFromFile(rolesetNoDecay, s))
            .filter(_._2)
        })
      val roles = groundTruthExamples
        .flatMap(e => Set(e._1.v1,e._1.v2))
        .toSet
      groundTruthExamples
        .sortBy(_._1.v1.roleLineage.toRoleLineage.nonWildcardValueSetBefore(trainTimeEnd).toIndexedSeq.map(_.toString).sorted.head.toString)
        .foreach(l => println(l._1.firstNonWildcardValueOverlap,l._1.v1.wikipediaURL + s" (${l._1.v1.property})",l._1.v2.wikipediaURL + s" (${l._1.v2.property})"))
      val buffyRoles = roles
        .filter(_.roleLineage.toRoleLineage.nonWildcardValueSetBefore(trainTimeEnd)
          .exists(v => v.toString.contains("Buffy")))
      val buffyEdges = groundTruthExamples.filter(e => buffyRoles.contains(e._1.v1) || buffyRoles.contains(e._1.v2))
        .map(_._1)
        .toSet
      val values = Set("The Office","Buffy","Twilight Zone","Science fiction","Fantasy fiction")
      //println("Buffy Roles",buffyRoles.size,"Buffy Edges:",buffyEdges.size)
      val frequentBlockingKeys = GLOBAL_CONFIG.STANDARD_TIME_RANGE
        .filter(ld => ld.isBefore(trainTimeEnd))
        .flatMap(ld => {
          groundTruthExamples
            .flatMap(e => Set(e._1.v1,e._1.v2))
            .groupBy(rl => rl.roleLineage.toRoleLineage.valueAt(ld))
            .filter{case (k,l) => !RoleLineage.isWildcard(k)}
            .map{case (k,l) => ((ld,k),l)}
        })
        .filter{case ((ld,k),l) => l.size>2}
        .map{case ((ld,k),l) => k}
        .toSet
      val moreThanSingleEdge = frequentBlockingKeys
        .map(k => (k,groundTruthExamples
          .filter(e => Set(e._1.v1,e._1.v2).forall(rl => rl.roleLineage.toRoleLineage.nonWildcardValueSetBefore(trainTimeEnd).contains(k)))))
        .toIndexedSeq
        .filter(_._2.size>1)
        .sortBy(-_._2.size)
      //println("Sum of more than single edge:",moreThanSingleEdge.flatMap(_._2.map(_._1)).toSet.size)
//      moreThanSingleEdge
//        .foreach{case (k,l) => println(k,l.size)}
    }}
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
        val roleMatchStatistics = new RoleMatchStatistics(dataset,edgeNoDecay,label,DECAY_THRESHOLD,DECAY_THRESHOLD_SCB,trainTimeEnd)
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
