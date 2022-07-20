package de.hpi.role_matching.evaluation.exploration

import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.PrintWriter
import scala.io.Source

object DataCleaningCaseStudyMain extends App {
  //normal()

  private val invalidRoleMatchingExamplesDir = args(0)
  val fileName = args(1)
  val resultDir = args(2)
  val csvFile = invalidRoleMatchingExamplesDir + s"/${fileName}_invalid.csv"
  val vertexLookupMap = Roleset.fromJsonFile(args(1) + s"/$fileName.json").getStringToLineageMap
  val edges = Source
    .fromFile(csvFile)
    .getLines()
    .toIndexedSeq
    .tail
    .map(l => {
      val tokens = l.split(",")
      val id = tokens(5)
      val id2 = tokens(6)
      if (!vertexLookupMap.contains(id) || !vertexLookupMap.contains(id2))
        None
      else {
        val l1 = vertexLookupMap(id)
        val l2 = vertexLookupMap(id2)
        Some(SimpleCompatbilityGraphEdge(l1, l2))
      }
    })
    .flatten
  val resultPr = new PrintWriter(resultDir + s"/$fileName.txt")
  edges.foreach(ge => {
    val str = ge.getTabularEventLineageString
    resultPr.println(str)
  })
  resultPr.close()
}
