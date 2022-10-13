package de.hpi.role_matching.data

import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.role_matching.evaluation.matching.DittoResultToCSVMain
import de.hpi.role_matching.wikipedia_data_preparation.transformed.ExtendedWikipediaRoleID

import scala.io.Source

case class LabelledRoleMatchCandidate(id1:String,id2:String,isTrueRoleMatch:Boolean) extends JsonWritable[LabelledRoleMatchCandidate]

object LabelledRoleMatchCandidate extends JsonReadable[LabelledRoleMatchCandidate] {

  def getIDAndProperty(str: String) = {
    val id = BigInt(str
      .split("COL EID VAL ")(1)
      .split("COL")(0)
      .trim)
    val p = str
      .split("COL PID VAL ")(1)
      .split("COL")(0)
      .trim
    (id,p)
  }

  def fromDittoFile(inputFileTrain: String, rs:Roleset) :Iterator[LabelledRoleMatchCandidate]= {
    val map = rs
      .rolesSortedByID
      .map(id => (ExtendedWikipediaRoleID.Optionfrom(id),id))
      .withFilter(_._1.isDefined)
      .map(t => (t._1.get,t._2))
      .map{case (idObject,id) => ((idObject.pageID,Util.toDittoSaveString(idObject.p)),id)}
      .toMap
    //val map = mapRaw.map(t => (t._1,t._2.head._2))
    if(map.size < rs.rolesSortedByID.size){
      println((rs.rolesSortedByID.size - map.size) / rs.rolesSortedByID.size.toDouble)
    }
    Source
      .fromFile(inputFileTrain)
      .getLines()
      .map{s =>
        val tokensFirstSplit = s.split("\t")
        val label = tokensFirstSplit(2) == "1"
        val r1 = getIDAndProperty(tokensFirstSplit(0))
        val r2 = getIDAndProperty(tokensFirstSplit(1))
        if(!map.contains(r1) || !map.contains(r2)) {
          println()
        }
        LabelledRoleMatchCandidate(map(r1),map(r2),label)
      }
  }

}
