package de.hpi.role_matching.ditto

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}
import de.hpi.role_matching.evaluation.tuning.BasicStatRow

import java.io.{File, PrintWriter}
import java.time.LocalDate

class DittoExporter(vertices: Roleset, trainTimeEnd: LocalDate,resultFile:File) extends StrictLogging{

  val vertexMap = vertices.getStringToLineageMap.map{case (k,v) => (k,v.roleLineage.toRoleLineage)}
  val vertexMapOnlyTrain = vertexMap.map{case (k,v) => (k,v.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))}
  val vertexMapOnlyTrainWithID = vertices.getStringToLineageMap.map{case (k,v) => (k,RoleLineageWithID(v.id,v.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd).toSerializationHelper))}


  val resultPr = new PrintWriter(resultFile)

  def exportData() = {
    val blocks:IndexedSeq[IndexedSeq[String]] = blocking()
//    blocks.foreach{ block =>
//      for(i <- 0 until block.size){
//        for(j <- i until block.size){
//          val v1 = block(i)
//          val v2 = block(j)
//          val label:Option[Boolean] = getClassLabel(v1,v2)
//          if(label.isDefined){
//            outputRecord(v1,v2,label.get)
//          }
//        }
//      }
//    }
//    resultPr.close()
  }

  def blocking(): IndexedSeq[IndexedSeq[String]] = {
    val blocks = vertexMapOnlyTrainWithID
      .groupBy(t => vertexMapOnlyTrain(t._1).nonWildCardValues.toSet)
      .map{case (k,v) => v.values.map(_.id).toIndexedSeq}
      .toIndexedSeq
    val totalNumberOfEdges = blocks.map(b => (b.size*(b.size-1))/2).sum
    logger.debug(s"Will create $totalNumberOfEdges")
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
