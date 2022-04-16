package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate
import scala.io.Source

object DebugMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val path = "/home/leon/tmp/edges.json"
  val vertexMap = Roleset.fromJsonFile("/home/leon/data/dataset_versioning/finalExperiments/cleaning_tool_files/currentWikipediaRolesetForSampling/politics.json")
  val alternateRoleset = Roleset.fromJsonFile("/home/leon/tmp/politics.json")
  process("/home/leon/data/dataset_versioning/finalExperiments/semanticAnnotation/annotationFiles/politics/2022-04-13T10:33:56.440879_politics_annotations.csv")
  //process("/home/leon/data/dataset_versioning/finalExperiments/semanticAnnotation/annotationFiles/politics/2022-03-25T11:14:04.356829_politics_annotations.csv")

  private def process(path:String) = {
    val sampleTargetCount = new SampleTargetCount(85,40,74)
    val edgesOutput = Source.fromFile(path)
      .getLines()
      .toIndexedSeq
      .tail
      .map(l => (l.split(",")(0).toInt, l.split(",")(1).toInt))
      .map(t => SimpleCompatbilityGraphEdgeID(vertexMap.positionToRoleLineage(t._1).id, vertexMap.positionToRoleLineage(t._2).id))
      .toSet
    val trainTimeEnd = LocalDate.parse("2016-05-07")
    val simpleBlockingSampler = new SimpleBlockingSampler(new File("."),".",trainTimeEnd,13,Map(("politics",sampleTargetCount)),new File("."),None)
    val vertexMapNormal = vertexMap.getStringToLineageMap
      .map { case (k, v) => (k, v.roleLineage.toRoleLineage) }
    val vertexMapProjected = vertexMap.getStringToLineageMap
      .map { case (k, v) => (k, v.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, trainTimeEnd)) }
    val vertexMapAlternate = alternateRoleset.getStringToLineageMap
      .map { case (k, v) => (k, v.roleLineage.toRoleLineage) }
    val targetCount = SampleTargetCount(0, 0, 0)
    val sample = collection.mutable.HashSet[SimpleCompatbilityGraphEdgeID]()
    val res = edgesOutput.map(e => {
      val time1 = vertexMapNormal(e.v1).getCompatibilityTimePercentage(vertexMapNormal(e.v2), trainTimeEnd)
      val time2 = vertexMapProjected(e.v1).getCompatibilityTimePercentage(vertexMapProjected(e.v2), trainTimeEnd)
      val time3 = vertexMapAlternate(e.v1).getCompatibilityTimePercentage(vertexMapAlternate(e.v2), trainTimeEnd)
      //println(s"${e.v1},${e.v2},$time3")
      targetCount.reduceNeededCount(time1)
      simpleBlockingSampler.addSampleIfValid(vertexMapAlternate,sampleTargetCount,Set(),sample,e.v1,e.v2)
      time1 == time3
    })
      .filter(!_)
    println(sampleTargetCount)
    println(res.size)
    println(targetCount)
  }
}
