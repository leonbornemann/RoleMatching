package de.hpi.oneshot

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}
import de.hpi.role_matching.evaluation.tuning.BasicStatRow

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.{Codec, Source}
import scala.sys.process._


class DittoExporterFromRM(inputDir: File,
                          resultFile: File,
                          tmpOutputDir:File,
                          rolesetFile: String,
                          trainTimeEnd:LocalDate,
                          exportEntityPropertyIDs: Boolean,
                          exportSampleOnly: Boolean,
                          maxSampleSize: Int) {

  tmpOutputDir.mkdirs()

  val vertices = Roleset.fromJsonFile(rolesetFile)
  val vertexMap = vertices.getStringToLineageMap.map{case (k,v) => (k,v.roleLineage.toRoleLineage)}
  val vertexMapOnlyTrain = vertexMap.map{case (k,v) => (k,v.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))}
  val vertexMapOnlyTrainWithID = vertices.getStringToLineageMap.map{case (k,v) => (k,RoleLineageWithID(v.id,v.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd).toSerializationHelper))}
  val vertexMapOnlyEvaluation = vertexMap.map{case (k,v) => (k,v.projectToTimeRange(trainTimeEnd,GLOBAL_CONFIG.STANDARD_TIME_FRAME_END))}
  val evaluationPeriod = GLOBAL_CONFIG.STANDARD_TIME_RANGE.filter(_.isAfter(trainTimeEnd))
  val transitionSets = vertexMapOnlyTrain.map{case (k,l) => (k,l.informativeValueTransitions)}
  val resultPr = new PrintWriter(resultFile)

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

  private def getIDString(id1: String) = {
    if (exportEntityPropertyIDs)
      Some(RoleLineageWithID.getDittoIDString(id1))
    else
      None
  }

  def outputRecord(id1:String, id2:String, label: Boolean) = {
    val idString1 = getIDString(id1)
    val idString2 = getIDString(id2)
    val output1 = vertexMapOnlyTrain(id1).dittoString(trainTimeEnd,idString1)
    val output2 = vertexMapOnlyTrain(id2).dittoString(trainTimeEnd,idString2)
    val labelString = if(label) "1" else "0"
    val finaloutPutString = if(id1 < id2)
      output1 + "\t" + output2 + "\t" + labelString
    else
      output2 + "\t" + output1 + "\t" + labelString
    resultPr.println(finaloutPutString)
  }

  def satisfiesTransitionFilter(v1: String, v2: String): Boolean = {
    transitionSets(v1).intersect(transitionSets(v2)).size>0
  }

  def prepareSample() = {
    val shuffledInputFile = s"${tmpOutputDir.getAbsolutePath}/${inputDir.getName}.csv"
    val command1 = Seq("/bin/sh", "-c", "sed '1d;$d' "+ s"${inputDir.getAbsolutePath}/* > $shuffledInputFile")
    //val command1 = s"cat ${inputDir.getAbsolutePath}/* > $shuffledInputFile"
    val command2 = s"shuf $shuffledInputFile -o $shuffledInputFile"
    val outputConcat = command1.!
    println(s"Output of concat command: $outputConcat")
    val outputShuffle = command2.!
    println(s"Output of shuffle command: $outputShuffle")
  }

  def exportSample() = {
    val shuffledInputFile = s"${tmpOutputDir.getAbsolutePath}/${inputDir.getName}.csv"
    //first shuffle input file
    val inputLineIterator = Source.fromFile(shuffledInputFile)(Codec.UTF8.charSet)
      .getLines()
    var serialized = 0
    var curLineNumber = 0
    while(inputLineIterator.hasNext && serialized < maxSampleSize){
      val curLine = inputLineIterator.next().split(",")
      if(curLine.size==3){
        val v1 = curLine(0)
        val v2 = curLine(1)
        if(vertexMap.contains(v1)&& vertexMap.contains(v2) && satisfiesTransitionFilter(v1,v2)){
          val label:Option[Boolean] = getClassLabel(v1,v2)
          if(label.isDefined){
            SimpleCompatbilityGraphEdgeID(v1,v2).appendToWriter(resultPr,false,true)
            //outputRecord(v1,v2,label.get)
            serialized +=1
          }
        }
      }
      curLineNumber += 1
      println(curLineNumber)
    }
    resultPr.close()
    executeTrainTestSplit(serialized)
  }


}
