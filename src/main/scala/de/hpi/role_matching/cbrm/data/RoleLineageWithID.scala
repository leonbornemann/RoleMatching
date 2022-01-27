package de.hpi.role_matching.cbrm.data

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.RoleLineageWithID.digitRegex
import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.util.TableFormatter

case class RoleLineageWithID(id:String, factLineage: RoleLineageWithHashMap) extends JsonWritable[RoleLineageWithID] {

  def csvSafeID = id.replace('\r',' ').replace('\n',' ').replace(',',' ')

  def isNumeric = {
    factLineage.lineage.values.forall(v => RoleLineage.isWildcard(v) || GLOBAL_CONFIG.nonInformativeValues.contains(v) || v.toString.matches(digitRegex))
  }

}

object RoleLineageWithID extends JsonReadable[RoleLineageWithID] {

  def toReferences(lineages: IndexedSeq[RoleLineageWithID]) = {
    val sortedByID = lineages
      .sortBy(_.id)
      .zipWithIndex
    val asMap = sortedByID.map(t => (t._2, t._1)).toMap
    val roleset = Roleset(sortedByID.map(_._1.id),asMap)
    asMap.toIndexedSeq.sortBy(_._1).map(t => RoleReference(roleset.toNonCaseClass,t._1))
  }

  def printTabularEventLineageString(vertices:collection.Seq[RoleLineageWithID]) = {
    println(getTabularEventLineageString(vertices))
  }

  def getTabularEventLineageString(vertices:collection.Seq[RoleLineageWithID]):String = {
    val allDates = vertices.flatMap(_.factLineage.lineage.keySet).sortBy(_.toEpochDay)
    val header = Seq("") ++ allDates
    val cellsAll = vertices.map(v => {
      Seq(v.id) ++ allDates.map(t => v.factLineage.toRoleLineage.valueAt(t)).map(v => if(RoleLineage.isWildcard(v)) "_" else v)
    }).toSeq
    TableFormatter.format(Seq(header) ++ cellsAll)
  }


  val digitRegex = "[0-9]+"

  def getTransitionHistogramForTFIDFFromVertices(vertices:Seq[RoleLineageWithID], granularityInDays:Int) :Map[ValueTransition,Int] = {
    val allTransitions = vertices
      .flatMap( (v:RoleLineageWithID) => {
        val transitions = v.factLineage.toRoleLineage.getValueTransitionSet(true,granularityInDays).toSeq
        transitions
      })
    allTransitions
      .groupBy(identity)
      .map(t => (t._1,t._2.size))
  }

}
