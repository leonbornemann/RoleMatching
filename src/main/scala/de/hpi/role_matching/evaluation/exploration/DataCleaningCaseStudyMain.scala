package de.hpi.role_matching.evaluation.exploration

import de.hpi.role_matching.clique_partitioning.ScoreConfig
import de.hpi.role_matching.compatibility.graph.representation.simple.GeneralEdge
import de.hpi.role_matching.compatibility.graph.representation.slim.VertexLookupMap

import java.io.PrintWriter
import scala.io.Source

object DataCleaningCaseStudyMain extends App {
  val dsName = "military"
  val csvFile = args(0) + s"/${dsName}_invalid.csv"
  val vertexLookupMap = VertexLookupMap.fromJsonFile(args(1) + s"/$dsName.json").getStringToLineageMap
  val resultDir = "/home/leon/data/dataset_versioning/cliqueEvaluation/InvalidExamples"
  val edges = Source
    .fromFile(csvFile)
    .getLines()
    .toIndexedSeq
    .tail
    .map(l => {
      val tokens = l.split(",")
      val id = tokens(5)
      val id2 = tokens(6)
      val l1 = vertexLookupMap(id)
      val l2 = vertexLookupMap(id2)
      GeneralEdge(l1,l2)
    })
  val resultPr = new PrintWriter(resultDir + s"/$dsName.txt")
  edges.foreach(ge => {
    val str = ge.getTabularEventLineageString
    resultPr.println(str)
  })
  resultPr.close()
}
