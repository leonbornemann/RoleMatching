package de.hpi.role_matching.cbrm.compatibility_graph.representation.simple

import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}
import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch.{FactLineage, ValueTransition}
import de.hpi.socrata.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.socrata.JsonReadable
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.data.RoleLineageWithID
import de.hpi.role_matching.evaluation.tuning
import de.hpi.util.TableFormatter

case class SimpleCompatbilityGraphEdge(v1:RoleLineageWithID, v2:RoleLineageWithID) extends JsonWritable[SimpleCompatbilityGraphEdge] {

  def toGeneralEdgeStatRow(granularityInDays: Int,
                           trainGraphConfig: GraphConfig,
                           nonInformativeValues:Set[Any],
                           transitionHistogramForTFIDF:Map[ValueTransition[Any],Int],
                           lineageCount:Int) = {
    tuning.EdgeStatRow(granularityInDays,
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
    println(getTabularEventLineageString)
  }

  def getTabularEventLineageString = {
    val dates = v1.factLineage.toFactLineage.lineage.keySet//.filter(v => !FactLineage.isWildcard(v._2) && v._2!="").keySet
    val dates2 = v2.factLineage.toFactLineage.lineage.keySet//.filter(v => !FactLineage.isWildcard(v._2) && v._2!="").keySet
    val allDates = dates.union(dates2).toIndexedSeq.sorted
    val cells1 = IndexedSeq(v1.id) ++ allDates.map(t => v1.factLineage.toFactLineage.valueAt(t)).map(v => if(FactLineage.isWildcard(v)) "_" else v)
    val cells2 = IndexedSeq(v2.id) ++ allDates.map(t => v2.factLineage.toFactLineage.valueAt(t)).map(v => if(FactLineage.isWildcard(v)) "_" else v)
    val valuesMatch = (1 until cells1.size).map(i => {
      cells1(i)=="_" || cells2(i)=="_" || cells1(i)==cells2(i)
    })
    val header = Seq("") ++ allDates.zip(valuesMatch).map(t => t._1.toString + (if(t._2) "" else " (!)") )
    TableFormatter.format(Seq(header) ++ Seq(cells1,cells2))
  }
}

object SimpleCompatbilityGraphEdge extends JsonReadable[SimpleCompatbilityGraphEdge] {

  def getTransitionHistogramForTFIDF(edges:Iterable[SimpleCompatbilityGraphEdge], granularityInDays:Int) :Map[ValueTransition[Any],Int] = {
    RoleLineageWithID.getTransitionHistogramForTFIDFFromVertices(edges.flatMap(ge => Seq(ge.v1,ge.v2)).toSet.toSeq,granularityInDays)
  }

  def getLineageCount(edges:Iterable[SimpleCompatbilityGraphEdge]) = edges.map(e => Seq(e.v1.id,e.v2.id)).toSet.size

}
