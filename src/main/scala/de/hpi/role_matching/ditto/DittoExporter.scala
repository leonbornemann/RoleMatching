package de.hpi.role_matching.ditto

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.blocking.EMBlocking
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.{SimpleCompatbilityGraphEdge, SimpleCompatbilityGraphEdgeID}
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}
import de.hpi.role_matching.evaluation.tuning.BasicStatRow

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source
import scala.sys.process._
import scala.util.Random


class DittoExporter(vertices: Roleset,
                    trainTimeEnd: LocalDate,
                    blocker:Option[EMBlocking],
                    resultFile:File,
                    exportEntityPropertyIDs:Boolean,
                    exportEvidenceCounts:Boolean,
                    exportSampleOnly:Boolean,
                    maxCandidatePairs:Int = Integer.MAX_VALUE) extends DittoDataExporter with StrictLogging{

  val random = new Random(13)
  val vertexMap = vertices.getStringToLineageMap.map{case (k,v) => (k,v.roleLineage.toRoleLineage)}
  val vertexMapOnlyTrain = vertexMap.map{case (k,v) => (k,v.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))}
  val vertexMapOnlyTrainWithID = vertices.getStringToLineageMap.map{case (k,v) => (k,RoleLineageWithID(v.id,v.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd).toSerializationHelper))}
  val vertexMapOnlyEvaluation = vertexMap.map{case (k,v) => (k,v.projectToTimeRange(trainTimeEnd,GLOBAL_CONFIG.STANDARD_TIME_FRAME_END))}
  val tfIDFMap = RoleLineageWithID.getTransitionHistogramForTFIDFFromVertices(vertexMapOnlyTrainWithID.values.toSeq, GLOBAL_CONFIG.granularityInDays)
  val evaluationPeriod = GLOBAL_CONFIG.STANDARD_TIME_RANGE.filter(_.isAfter(trainTimeEnd))

  val idToRoleLineageSmallestTrainTimeEnd = vertexMap.map{case (id,rl) => (id,rl.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))}
  val idToChangeSetInSmallestTrainTimeEnd = idToRoleLineageSmallestTrainTimeEnd.map{case (id,rl) => (id,rl.allNonWildcardTimestamps.toSet)}

  val transitionSets = if(exportEvidenceCounts) {
    Some(vertexMapOnlyTrain
      .map{case (k,rl) => (k,rl.valueTransitions(true,false))}.toMap)
  } else
    None

  val resultPr = new PrintWriter(resultFile)

  def exportDataForMatchFile(e:Iterator[SimpleCompatbilityGraphEdgeID]) = {
    var exportedLines =0
    e.foreach(e => {
      val (v1,v2) = (e.v1,e.v2)
      val label:Option[Boolean] = getClassLabel(v1,v2)
      if(label.isDefined){
        outputRecord(v1,v2,label.get)
        exportedLines +=1
      }
    })
    resultPr.close()
    executeTrainTestSplit(exportedLines)
  }

  //val maxTrainingExampleCount = approximateSampleSize.getOrElse(Int.MaxValue)

  def exportDataForGraph(e:Iterator[SimpleCompatbilityGraphEdge]) = {
    var exportedLines =0
    e.foreach(e => {
      val (v1,v2) = (e.v1.id,e.v2.id)
      val label:Option[Boolean] = getClassLabel(v1,v2)
      if(label.isDefined){
        outputRecord(v1,v2,label.get)
        exportedLines +=1
      }
    })
    resultPr.close()
    executeTrainTestSplit(exportedLines)
  }

  def executeTrainTestSplit(exportedLines: Int) = {
    val filename = resultFile.getAbsolutePath
    //shuffle file
    val cmd = s"shuf $filename -o $filename" // Your command
    val output = cmd.!
    println(s"Output of shuffle command: $output")
    //train / validation / test split
    val trainFile = new PrintWriter(s"${filename}_train.txt")
    val validationFile = new PrintWriter(s"${filename}_validation.txt")
    val testFile = new PrintWriter(s"${filename}_test.txt")
    val validationLineBegin = (exportedLines*0.6).toInt
    val testLineBegin = (exportedLines*0.8).toInt
    var curCount = 0
    Source.fromFile(resultFile).getLines().foreach{s =>
      if(curCount<validationLineBegin)
        trainFile.println(s)
      else if (curCount<testLineBegin)
        validationFile.println(s)
      else
        testFile.println(s)
      curCount+=1
    }
    trainFile.close()
    validationFile.close()
    testFile.close()
  }

  def exportDataWithSimpleBlocking() = {
    val blocks:IndexedSeq[IndexedSeq[String]] = blocking()
    var exportedLines = 0
    var blockI = 0
    if(!exportSampleOnly){
      while(exportedLines<maxCandidatePairs && blockI!=blocks.size){
        var pairI = 0
        val curBlock = blocks(blockI)
        while(exportedLines<maxCandidatePairs  && pairI<curBlock.size-1){
          var pairJ = pairI+1
          while(exportedLines<maxCandidatePairs  && pairJ<curBlock.size){
            val v1 = curBlock(pairI)
            val v2 = curBlock(pairJ)
            val label:Option[Boolean] = getClassLabel(v1,v2)
            if(label.isDefined){
              outputRecord(v1,v2,label.get)
              exportedLines +=1
            }
            pairJ+=1
          }
          pairI+=1
        }
        blockI+=1
      }
    } else {
      val sampledPairs = collection.mutable.HashSet[(String,String)]()
      blocks
        .foreach(b => {
          for (i <- 0 until b.size){
            for (j <- i+1 until b.size){
              val v1 = b(i)
              val v2 = b(j)
              val label:Option[Boolean] = getClassLabel(v1,v2)
              if(label.isDefined){
                outputRecord(v1,v2,label.get)
                exportedLines +=1
                sampledPairs.add((v1,v2))
              }
            }
          }
        })
    }
    resultPr.close()
    executeTrainTestSplit(exportedLines)
  }

  def blocking(): IndexedSeq[IndexedSeq[String]] = {
    blocker.get.idGroups
      .map(_._2)
      .toIndexedSeq
  }


  def outputRecord(id1:String, id2:String, label: Boolean) = {
    val idString1 = getIDString(id1)
    val idString2 = getIDString(id2)
    val output1 = vertexMapOnlyTrain(id1).dittoString(trainTimeEnd,idString1)
    val output2 = vertexMapOnlyTrain(id2).dittoString(trainTimeEnd,idString2)
    val labelString = if(label) "1" else "0"
    val finaloutPutString = if(id1 < id2)
      " " + output1 + "\t" + output2 + "\t" + labelString
    else
      " " + output2 + "\t" + output1 + "\t" + labelString
    resultPr.println(finaloutPutString)
  }

  private def getIDString(id1: String) = {
    if (exportEntityPropertyIDs)
      Some(RoleLineageWithID.getDittoIDString(id1))
    else
      None
  }

  def getClassLabel(v1:String, v2:String): Option[Boolean] = {
    val statRow = new BasicStatRow(vertexMap(v1), vertexMap(v2), trainTimeEnd)
    val hasEvidence = statRow.isInterestingInEvaluation
//    val evidence = idToChangeSetInSmallestTrainTimeEnd(e.v1.id).intersect(idToChangeSetInSmallestTrainTimeEnd(e.v2.id)).size
//    evidence>0
    if(hasEvidence)
      Some(vertexMap(v1).exactlyMatchesWithoutDecayInTimePeriod(vertexMap(v2),evaluationPeriod))
    else
      None
  }


}
