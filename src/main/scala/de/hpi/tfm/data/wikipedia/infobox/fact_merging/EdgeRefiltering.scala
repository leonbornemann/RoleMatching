package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.wikipedia.infobox.statistics.edge.EdgeAnalyser
import de.hpi.tfm.evaluation.data.GeneralEdge
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.IOService

import java.io.PrintWriter
import java.time.LocalDate

object EdgeRefiltering extends App {
  val inputFile = args(0)
  val outputFile = args(1)
  val statOutputFile = args(2)
  val timeBegin = LocalDate.parse(args(3))
  val trainTimeEnd = LocalDate.parse(args(4))
  val timeEnd = LocalDate.parse(args(5))
  val timestampResulotionInDays = args(6).toInt
  IOService.STANDARD_TIME_FRAME_START=timeBegin
  IOService.STANDARD_TIME_FRAME_END=timeEnd
  val nonInformativeValues:Set[Any] = Set("",null)
  val edges = GeneralEdge.fromJsonObjectPerLineFile(inputFile)
  val filteredEdges = edges.filter(e => {
    val transitions1 = e.v1.factLineage.toFactLineage.projectToTimeRange(timeBegin,trainTimeEnd).valueTransitions(false,true)
      .filter(t => !nonInformativeValues.contains(t.prev) && !nonInformativeValues.contains(t.after))
    val transitions2 = e.v2.factLineage.toFactLineage.projectToTimeRange(timeBegin,trainTimeEnd).valueTransitions(false,true)
      .filter(t => !nonInformativeValues.contains(t.prev) && !nonInformativeValues.contains(t.after))
    !transitions1.intersect(transitions2).isEmpty
  })
  val pr = new PrintWriter(outputFile)
  filteredEdges.foreach(_.appendToWriter(pr,false,true))
  pr.close()
  val analyser = new EdgeAnalyser(edges,GraphConfig(1,timeBegin,trainTimeEnd),timestampResulotionInDays,GLOBAL_CONFIG.nonInformativeValues)
}
