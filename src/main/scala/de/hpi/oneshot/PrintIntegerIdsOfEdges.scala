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

  println("Diverse")
  printIDs(pathGs1)
  println("Representative")
  printIDs(pathGs2)
}
