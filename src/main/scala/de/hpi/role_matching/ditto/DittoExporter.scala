package de.hpi.role_matching.ditto

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}
import de.hpi.role_matching.evaluation.tuning.BasicStatRow

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source
import scala.sys.process._


class DittoExporter(vertices: Roleset, trainTimeEnd: LocalDate,resultFile:File) extends StrictLogging{

  val vertexMap = vertices.getStringToLineageMap.map{case (k,v) => (k,v.roleLineage.toRoleLineage)}
  val vertexMapOnlyTrain = vertexMap.map{case (k,v) => (k,v.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))}
  val vertexMapOnlyTrainWithID = vertices.getStringToLineageMap.map{case (k,v) => (k,RoleLineageWithID(v.id,v.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd).toSerializationHelper))}

  val resultPr = new PrintWriter(resultFile)

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
    blocks.foreach{ block =>
      for(i <- 0 until block.size){
        for(j <- i until block.size){
          val v1 = block(i)
          val v2 = block(j)
          val label:Option[Boolean] = getClassLabel(v1,v2)
          if(label.isDefined){
            outputRecord(v1,v2,label.get)
            exportedLines +=1
          }
        }
      }
    }
    resultPr.close()
    exportedLines
    executeTrainTestSplit(exportedLines)
  }

  def blocking(): IndexedSeq[IndexedSeq[String]] = {
    val grouped = vertexMapOnlyTrainWithID
      .groupBy(t => vertexMapOnlyTrain(t._1).nonWildcardValueSequenceBefore(trainTimeEnd))
    val blocks = grouped
      .map{case (k,v) => v.values.map(_.id).toIndexedSeq}
      .toIndexedSeq
    val totalNumberOfEdges = blocks.map(b => (b.size*(b.size-1))/2).sum
    logger.debug(s"Will create $totalNumberOfEdges")
    //val exampleProbability = if(totalNumberOfEdges<maxTrainingExampleCount)
    val topBlocks = grouped
      .map{case (k,v) => (k,v.size)}
      .toIndexedSeq
      .sortBy(-_._2)
      .take(20)
    topBlocks.foreach{case (k,v) => logger.debug(s"$v: $k")}
    blocks
  }

  def outputRecord(id1:String, id2:String, label: Boolean) = {
    val output1 = vertexMapOnlyTrain(id1).dittoString(trainTimeEnd)
    val output2 = vertexMapOnlyTrain(id2).dittoString(trainTimeEnd)
    val labelString = if(label) "1" else "0"
    val finaloutPutString = if(id1 < id2)
      output1 + "\t" + output2 + "\t" + labelString
    else
      output2 + "\t" + output1 + "\t" + labelString
    resultPr.println(finaloutPutString)
  }

  def getClassLabel(v1:String,v2:String): Option[Boolean] = {
    val statRow = new BasicStatRow(vertexMap(v1), vertexMap(v2), trainTimeEnd)
    val hasEvidence = statRow.isInteresting
    if(hasEvidence)
      Some(statRow.remainsValidFullTimeSpan)
    else
      None
  }


}
