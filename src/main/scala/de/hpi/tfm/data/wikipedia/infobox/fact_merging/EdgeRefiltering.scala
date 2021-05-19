package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import de.hpi.tfm.evaluation.data.GeneralEdge
import de.hpi.tfm.io.IOService

import java.io.PrintWriter
import java.time.LocalDate

object EdgeRefiltering extends App {
  val inputFile = args(0)
  val outputFile = args(1)
  val timeBegin = LocalDate.parse(args(2))
  val trainTimeEnd = LocalDate.parse(args(3))
  val timeEnd = LocalDate.parse(args(4))
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

}
