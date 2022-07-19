package de.hpi.role_matching.evaluation.matching

import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}

import java.io.{File, PrintWriter}
import scala.io.Source

object DittoResultJsonToPythonCSV extends App {
//  val l = "COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL leftarm2  COL 2003-01-04 VAL \u200C⊥R\u200C COL D0 VAL 1932  COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL leftarm2  COL 2008-04-19 VAL \u200C⊥CE\u200C COL D1 VAL 1848  COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL leftarm2  COL 2013-05-11 VAL ffff00 COL D2 VAL 336  COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL leftarm2  COL 2014-04-12 VAL FFFF00 COL D3 VAL 14  COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL leftarm2  COL 2014-04-26 VAL FFD700 COL D4 VAL 448  COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL leftarm2  COL 2015-07-18 VAL FFDE00 COL D5 VAL 294\t COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL shorts2  COL 2003-01-04 VAL \u200C⊥R\u200C COL D0 VAL 1932  COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL shorts2  COL 2008-04-19 VAL \u200C⊥CE\u200C COL D1 VAL 1848  COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL shorts2  COL 2013-05-11 VAL ffff00 COL D2 VAL 336  COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL shorts2  COL 2014-04-12 VAL FFFF00 COL D3 VAL 14  COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL shorts2  COL 2014-04-26 VAL FFD700 COL D4 VAL 448  COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL shorts2  COL 2015-07-18 VAL FFDE00 COL D5 VAL 294\t1"
//  val rs = Roleset.fromJsonFile("/home/leon/data/dataset_versioning/finalExperiments/nodecayrolesets_wikipedia/football.json")
//  val byPageID = rs.rolesSortedByID.groupBy(id => RoleLineageWithID.getPageIDFromID(id))
//  val tokens = l.split("\\t")
//  val id1 = getIdParts(tokens(0),byPageID)
//  val id2 = getIdParts(tokens(1),byPageID)
//  val isMatch = tokens(2).toInt==1
//  println()


  val dittoDir = new File(args(0))
  val testFileDir = new File(args(1))
  val rolesetDir = new File(args(2))
  val resultDir = new File(args(3))
  val datasets = args(4).split(",")
  resultDir.mkdirs()

  def getIdParts(str: String,byPageID:Map[String,IndexedSeq[String]]) = {
      //val template = str.split("COL TID VAL ")(1).split(" COL EID VAL ")(0).trim.replace('_',' ')
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

  dittoDir
    .listFiles()
    .foreach(f => {
      val filename = f.getName.split("\\.")(0)
      val dsName = if(!filename.startsWith("tv")) filename.split("_")(0) else "tv_and_film"
      if(datasets.contains(dsName)){
        val rs = Roleset.fromJsonFile(rolesetDir + s"/$dsName.json")
        val byPageID = rs.rolesSortedByID.groupBy(id => RoleLineageWithID.getPageIDFromID(id))
        val resultPR = new PrintWriter(resultDir + s"/${dsName}.csv")
        DittoResult.appendSchema(resultPR)
        val dittoTestFile = testFileDir.getAbsolutePath + s"/$dsName.json.txt_test.txt"
        //TODO: parse apart: COL TID VAL infobox_football_club_season COL EID VAL 16823523 COL PID VAL leftarm2  COL
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
