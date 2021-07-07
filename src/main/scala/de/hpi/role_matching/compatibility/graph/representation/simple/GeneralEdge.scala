package de.hpi.role_matching.compatibility.graph.representation.simple

import de.hpi.socrata.tfmp_input.table.nonSketch.{FactLineage, ValueTransition}
import de.hpi.socrata.{JsonReadable, JsonWritable}
import de.hpi.role_matching.compatibility.GraphConfig
import de.hpi.role_matching.compatibility.graph.representation.vertex.IdentifiedFactLineage
import de.hpi.role_matching.evaluation.edge
import de.hpi.util.TableFormatter

case class GeneralEdge(v1:IdentifiedFactLineage, v2:IdentifiedFactLineage) extends JsonWritable[GeneralEdge] {

  def toGeneralEdgeStatRow(granularityInDays: Int,
                           trainGraphConfig: GraphConfig,
                           nonInformativeValues:Set[Any],
                           transitionHistogramForTFIDF:Map[ValueTransition[Any],Int],
                           lineageCount:Int) = {
    edge.GeneralEdgeStatRow(granularityInDays,
      trainGraphConfig,
      v1.csvSafeID,
      v2.csvSafeID,
      v1.factLineage.toFactLineage,
      v2.factLineage.toFactLineage,
      nonInformativeValues,
      transitionHistogramForTFIDF,
      lineageCount)
  }

  def printTabularEventLineageString = {
    val dates = v1.factLineage.toFactLineage.lineage.keySet//.filter(v => !FactLineage.isWildcard(v._2) && v._2!="").keySet
    val dates2 = v2.factLineage.toFactLineage.lineage.keySet//.filter(v => !FactLineage.isWildcard(v._2) && v._2!="").keySet
    val allDates = dates.union(dates2).toIndexedSeq.sorted
    val header = Seq("") ++ allDates
    val cells1 = Seq(v1.id) ++ allDates.map(t => v1.factLineage.toFactLineage.valueAt(t)).map(v => if(FactLineage.isWildcard(v)) "_" else v)
    val cells2 = Seq(v2.id) ++ allDates.map(t => v2.factLineage.toFactLineage.valueAt(t)).map(v => if(FactLineage.isWildcard(v)) "_" else v)
    TableFormatter.printTable(header,Seq(cells1,cells2))
  }

}

object GeneralEdge extends JsonReadable[GeneralEdge] {

  def getTransitionHistogramForTFIDF(edges:Iterable[GeneralEdge],granularityInDays:Int) :Map[ValueTransition[Any],Int] = {
    IdentifiedFactLineage.getTransitionHistogramForTFIDFFromVertices(edges.flatMap(ge => Seq(ge.v1,ge.v2)).toSet.toSeq,granularityInDays)
  }

  def getLineageCount(edges:Iterable[GeneralEdge]) = edges.map(e => Seq(e.v1.id,e.v2.id)).toSet.size

}
