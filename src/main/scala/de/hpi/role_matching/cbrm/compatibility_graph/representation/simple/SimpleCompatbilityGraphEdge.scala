package de.hpi.role_matching.cbrm.compatibility_graph.representation.simple

import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.data._
import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.role_matching.cbrm.evidence_based_weighting.EventOccurrenceStatistics
import de.hpi.role_matching.evaluation.tuning
import de.hpi.util.TableFormatter

import java.time.LocalDate

//not very memory efficient but can easily be written in parallel
case class SimpleCompatbilityGraphEdge(v1:RoleLineageWithID, v2:RoleLineageWithID) extends JsonWritable[SimpleCompatbilityGraphEdge] {

  def eventOccurrences(trainTimeEnd:LocalDate):EventOccurrenceStatistics = {
    val commonPointOfInterestIterator = new CommonPointOfInterestIterator(v1.roleLineage.toRoleLineage, v2.roleLineage.toRoleLineage)
    val eventCounts = EventOccurrenceStatistics(trainTimeEnd)
    commonPointOfInterestIterator
      .withFilter(cp => !cp.prevPointInTime.isAfter(trainTimeEnd))
      .foreach(cp => {
        //val eventCounts = getEventCounts(cp, firstNode, secondNode)

      })
    ???
  }

  def getEdgeID = {
    if(v1.csvSafeID < v2.csvSafeID){
      v1.csvSafeID + "||" + v2.csvSafeID
    } else {
      v2.csvSafeID + "||" + v1.csvSafeID
    }
  }


  def toGeneralEdgeStatRow(granularityInDays: Int,
                           trainGraphConfig: GraphConfig,
                           nonInformativeValues:Set[Any],
                           transitionHistogramForTFIDF:Map[ValueTransition,Int],
                           lineageCount:Int) = {
    tuning.EdgeStatRow(granularityInDays,
      trainGraphConfig,
      v1.csvSafeID,
      v2.csvSafeID,
      v1.roleLineage.toRoleLineage,
      v2.roleLineage.toRoleLineage,
      nonInformativeValues,
      transitionHistogramForTFIDF,
      lineageCount)
  }

  def printTabularEventLineageString = {
    println(getTabularEventLineageString)
  }

  def getTabularEventLineageString = {
    val dates = v1.roleLineage.toRoleLineage.lineage.keySet//.filter(v => !FactLineage.isWildcard(v._2) && v._2!="").keySet
    val dates2 = v2.roleLineage.toRoleLineage.lineage.keySet//.filter(v => !FactLineage.isWildcard(v._2) && v._2!="").keySet
    val allDates = dates.union(dates2).toIndexedSeq.sorted
    val cells1 = IndexedSeq(v1.id) ++ allDates.map(t => v1.roleLineage.toRoleLineage.valueAt(t)).map(v => if(RoleLineage.isWildcard(v)) "_" else v)
    val cells2 = IndexedSeq(v2.id) ++ allDates.map(t => v2.roleLineage.toRoleLineage.valueAt(t)).map(v => if(RoleLineage.isWildcard(v)) "_" else v)
    val valuesMatch = (1 until cells1.size).map(i => {
      cells1(i)=="_" || cells2(i)=="_" || cells1(i)==cells2(i)
    })
    val header = Seq("") ++ allDates.zip(valuesMatch).map(t => t._1.toString + (if(t._2) "" else " (!)") )
    TableFormatter.format(Seq(header) ++ Seq(cells1,cells2))
  }
}

object SimpleCompatbilityGraphEdge extends JsonReadable[SimpleCompatbilityGraphEdge] {

  def getTransitionHistogramForTFIDF(edges:Iterable[SimpleCompatbilityGraphEdge], granularityInDays:Int) :Map[ValueTransition,Int] = {
    RoleLineageWithID.getTransitionHistogramForTFIDFFromVertices(edges.flatMap(ge => Seq(ge.v1,ge.v2)).toSet.toSeq,granularityInDays)
  }

  def getLineageCount(edges:Iterable[SimpleCompatbilityGraphEdge]) = edges.map(e => Seq(e.v1.id,e.v2.id)).toSet.size

}
