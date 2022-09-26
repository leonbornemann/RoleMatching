package de.hpi.role_matching.data

import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.util.TableFormatter

import java.io.File

//not very memory efficient but can easily be written in parallel
case class RoleMatchCandidate(v1:RoleLineageWithID, v2:RoleLineageWithID) extends JsonWritable[RoleMatchCandidate] {

  def toLabelledCandidate(isTrueMatch: Boolean) = {
    LabelledRoleMatchCandidate(v1.id,v2.id,isTrueMatch)
  }


  def firstNonWildcardValueOverlap = {
    new CommonPointOfInterestIterator(v1.roleLineage.toRoleLineage,v2.roleLineage.toRoleLineage)
      .withFilter(cp => cp.curValueA==cp.curValueB && !RoleLineage.isWildcard(cp.curValueA))
      .next()
      .curValueA
  }

  def getEdgeID = {
    if(v1.csvSafeID < v2.csvSafeID){
      v1.csvSafeID + "||" + v2.csvSafeID
    } else {
      v2.csvSafeID + "||" + v1.csvSafeID
    }
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

object RoleMatchCandidate extends JsonReadable[RoleMatchCandidate] {
  def iterableFromEdgeIDObjectPerLineDir(dir: File, roleset: Roleset) = {
    val map = roleset.getStringToLineageMap
    RoleMatchCandidateIds.iterableFromJsonObjectPerLineDir(dir)
      .map(e => RoleMatchCandidate(map(e.v1),map(e.v2)))
  }


  def getTransitionHistogramForTFIDF(edges:Iterable[RoleMatchCandidate], granularityInDays:Int) :Map[ValueTransition,Int] = {
    RoleLineageWithID.getTransitionHistogramForTFIDFFromVertices(edges.flatMap(ge => Seq(ge.v1,ge.v2)).toSet.toSeq,granularityInDays)
  }

  def getLineageCount(edges:Iterable[RoleMatchCandidate]) = edges.map(e => Seq(e.v1.id,e.v2.id)).toSet.size

}
