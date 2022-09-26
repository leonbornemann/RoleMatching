package de.hpi.role_matching.evaluation.blocking.ground_truth

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.data._
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source
import scala.util.Random

class RoleMatchEvaluator(rolesetFilesNoneDecayed: Array[File]) extends StrictLogging{

  val random = new Random(13)

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
          RoleMatchCandidateIds(tokens(1),tokens(2))
        })
      //skip schema line
      groundTruthExamplIterator.next()
      val groundTruthExamples = groundTruthExamplIterator
        .map(e => (RoleMatchCandidate(stringToLineageMap(e.v1),stringToLineageMap(e.v2)),false))
      val retained = serializeSample(dataset,groundTruthExamples,resultPr)
      resultPr.close()
    })
  }


  val trainTimeEnd = LocalDate.parse("2016-05-07")
  var DECAY_THRESHOLD = 0.57
  var DECAY_THRESHOLD_SCB = 0.50

  def getEdgeFromFile(rolesetNoDecay:Roleset,s: String) = {
    val tokens = s.split(",")
    val firstID = tokens(0).toInt
    val secondID = tokens(1).toInt
    val isTrueMatch = tokens(2).toBoolean
    val rl1 = rolesetNoDecay.positionToRoleLineage(firstID)
    val rl2 = rolesetNoDecay.positionToRoleLineage(secondID)
    (RoleMatchCandidate(rl1,rl2),isTrueMatch)
  }

  def toLineage(stringToLineageMap:Map[String,RoleLineageWithID],s: LabelledRoleMatchCandidate) = {
    val rl1 = stringToLineageMap(s.id1)
    val rl2 = stringToLineageMap(s.id2)
    (RoleMatchCandidate(rl1,rl2),s.isTrueRoleMatch)
  }

  def getSamplingGroup(decayedCompatibilityPercentage: Double) = if(decayedCompatibilityPercentage<0.7) "[0.0,0.7)" else if(decayedCompatibilityPercentage < 1.0) "[0.7,1.0)" else "1.0"

  def getDecayedEdgeFromUndecayedEdge(edgeNoDecay: RoleMatchCandidate, beta:Double):RoleMatchCandidate = {
    val rl1WithDecay = edgeNoDecay.v1.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    val rl2WithDecay = edgeNoDecay.v2.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    RoleMatchCandidate(RoleLineageWithID(edgeNoDecay.v1.id,rl1WithDecay.toSerializationHelper),RoleLineageWithID(edgeNoDecay.v2.id,rl2WithDecay.toSerializationHelper))
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

  def executeEvaluation(inputDir: Array[File], resultPR: PrintWriter,resultDirRetained:Option[File]=None) = {
    RoleMatchStatistics.appendSchema(resultPR)
    inputDir.foreach{case (inputLabelFile) => {
      logger.debug("Processing" + inputLabelFile)
      val dataset = inputLabelFile.getName.split("\\.")(0)
      val rolesetNoDecay = Roleset.fromJsonFile(rolesetFilesNoneDecayed.find(f => f.getName.contains(dataset)).get.getAbsolutePath)
      val map = rolesetNoDecay.getStringToLineageMap
      val groundTruthExamples = LabelledRoleMatchCandidate.fromJsonObjectPerLineFile(inputLabelFile.getAbsolutePath)
        .map(lc => toLineage(map,lc))
      val retainedEdges = serializeSample(dataset,groundTruthExamples.iterator,resultPR)
      if(resultDirRetained.isDefined){
        resultDirRetained.get.mkdirs()
        val pr = new PrintWriter(resultDirRetained.get.getAbsolutePath + s"/$dataset.json")
        retainedEdges
          .map(t => t._1.toLabelledCandidate(t._2))
          .foreach(_.appendToWriter(pr,false,true))
        pr.close()
      }

    }}
    resultPR.close()
  }

  def serializeSample(dataset:String,
                      groundTruthExamples: Iterator[(RoleMatchCandidate, Boolean)],
                      resultPR:PrintWriter) = {
    val retainedSamples = collection.mutable.ArrayBuffer[(RoleMatchCandidate, Boolean)]()
    groundTruthExamples
      //.filter(_._2)
      .foreach{case (edgeNoDecay,label)=> {
        val roleMatchStatistics = new RoleMatchStatistics(dataset,edgeNoDecay,label,DECAY_THRESHOLD,DECAY_THRESHOLD_SCB,trainTimeEnd)
        retainedSamples.append((edgeNoDecay,label))
        roleMatchStatistics.appendStatRow(resultPR)
        }
      }
    retainedSamples
  }

}
