package de.hpi.role_matching.evaluation.matching

import de.hpi.role_matching.data.{RoleLineageWithID, Roleset}

import java.io.{File, PrintWriter}
import scala.io.Source

object DittoResultJsonToCSVMain extends App {

  val dittoResultDir = new File(args(0))
  val dittoTestFileDir = new File(args(1))
  val rolesetDir = new File(args(2))
  val resultDir = new File(args(3))
  val datasetsToEvaluate = args(4).split(",")
  resultDir.mkdirs()

  def getIdParts(str: String,byPageID:Map[String,IndexedSeq[String]]) = {
      val entityID = str.split(" COL EID VAL ")(1).split(" COL PID VAL")(0).trim.replace('_',' ')
      val propertyName = str.split("COL PID VAL")(1).split(" COL")(0).trim.replace('_',' ')
      val roles = byPageID(entityID)
      val filtered = roles.filter(rID => rID.contains(entityID) && rID.contains(propertyName))
      if(filtered.size==1){
        Some(filtered.head)
      } else {
        None
      }
  }

  dittoResultDir
    .listFiles()
    .foreach(f => {
      val filename = f.getName.split("\\.")(0)
      val dsName = if(!filename.startsWith("tv")) filename.split("_")(0) else "tv_and_film"
      if(datasetsToEvaluate.contains(dsName)){
        val rs = Roleset.fromJsonFile(rolesetDir + s"/$dsName.json")
        val byPageID = rs.rolesSortedByID.groupBy(id => RoleLineageWithID.getPageIDFromID(id))
        val resultPR = new PrintWriter(resultDir + s"/${dsName}.csv")
        DittoResult.appendSchema(resultPR)
        val dittoTestFile = dittoTestFileDir.getAbsolutePath + s"/$dsName.json.txt_test.txt"
        val isMatchIterator = Source
          .fromFile(dittoTestFile)
          .getLines()
          .map(l => {
            val tokens = l.split("\\t")
            val id1 = getIdParts(tokens(0),byPageID)
            val id2 = getIdParts(tokens(1),byPageID)
            val isMatch = tokens(2).toInt==1
            (id1,id2,isMatch)
          })
        val dittoResultIterator = DittoResult
          .iterableFromJsonObjectPerLineFile(f.getAbsolutePath)
        dittoResultIterator
          .zip(isMatchIterator)
          .foreach{case (dr,(id1,id2,isMatch)) => resultPR.println(dr.toCSVLine(id1,id2,isMatch))}
        resultPR.close()
      }
    })

}
