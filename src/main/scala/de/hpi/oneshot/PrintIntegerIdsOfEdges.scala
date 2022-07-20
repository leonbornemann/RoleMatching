package de.hpi.oneshot

import de.hpi.role_matching.cbrm.data.Roleset

import scala.io.Source

object PrintIntegerIdsOfEdges extends App {
  val pathGs1 = "/home/leon/PycharmProjects/RoleMatchingEvaluation/gs1errors.csv"
  val pathGs2 = "/home/leon/PycharmProjects/RoleMatchingEvaluation/gs2errors.csv"
  val rolesetPath = "/home/leon/data/dataset_versioning/finalExperiments/nodecayrolesets_wikipedia/"

  def printIDs(pathGs1: String) = {
    Source.fromFile(pathGs1)
      .getLines()
      .toIndexedSeq
      .tail
      .groupBy(_.split(",")(1))
      .foreach{case (ds,lines) => {
        val rs = Roleset.fromJsonFile(rolesetPath + s"/$ds.json")
          .positionToRoleLineage
          .map{case (pos,rl) => (rl.id,pos)}
        lines.foreach(l => {
          val tokens = l.split(",")
          val id1 = tokens(2)
          val id2 = tokens(3)
          println(ds,rs(id1),rs(id2))
        })
      }}
  }
//
//  println("Diverse")
//  printIDs(pathGs1)
//  println("Representative")
//  printIDs(pathGs2)

  def printIDSFromString(rs:Map[String,Int],str: String) = {
    val lines = str.split("\n")
    lines.foreach(l => {
      val tokens = l.split(",")
      val id1 = tokens(1)
      val id2 = tokens(2)
      println(rs(id1),rs(id2))
    })
  }

  val rs = Roleset.fromJsonFile("/home/leon/data/dataset_versioning/finalExperiments/nodecayrolesets_wikipedia/football.json")
    .positionToRoleLineage
    .map{case (pos,rl) => (rl.id,pos)}
  printIDSFromString(rs,"9970,football player infobox||15113494||182927324-0||position,infobox football biography||30768178||412333907-0||position\n9982,football player infobox||27772383||369173182-0||position,infobox football biography||30768178||412333907-0||position\n9817,football player infobox||24016395||308479948-0||position,football player infobox||3270396||203413366-0||position")
}
