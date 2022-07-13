package de.hpi.role_matching.cbrm.data

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.RoleLineageWithID.digitRegex
import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}
import de.hpi.util.TableFormatter

import java.time.LocalDate

case class RoleLineageWithID(id:String, roleLineage: RoleLineageWithHashMap) extends JsonWritable[RoleLineageWithID] {
  def property = {
    id.split("\\|\\|").last
  }

  def wikipediaURL = {
    val pageID = id
      .split("\\|\\|")(1)
    s"https://en.wikipedia.org/?curid=$pageID"
  }


  def csvSafeID = Util.toCSVSafe(id)

  def isNumeric = {
    roleLineage.lineage.values.forall(v => RoleLineage.isWildcard(v) || GLOBAL_CONFIG.nonInformativeValues.contains(v) || v.toString.matches(digitRegex))
  }

}

object RoleLineageWithID extends JsonReadable[RoleLineageWithID] {
  def getDittoIDString(id1: String): String = {
    if(id1.contains("||")) {
      val tokens = id1.split("\\|\\|")
      val template = Util.toDittoSaveString(tokens(0))
      val pageID = Util.toDittoSaveString(tokens(1))
      val propertyID = Util.toDittoSaveString(tokens.last)
      s" COL TID VAL $template COL EID VAL $pageID COL PID VAL $propertyID "
    } else {
      val tokens = id1.split("\\.")
      val datasetID = Util.toDittoSaveString(tokens(tokens.size-2))
      val tokens2 = tokens.last.split("_")
      val colID = Util.toDittoSaveString(tokens2(1))
      val rowID = Util.toDittoSaveString(tokens2(2))
      s"COL TID VAL $datasetID COL CID VAL $colID COL EID VAL $rowID"
    }
  }

    //infobox_politician||2568491||177756823-0||term_start_🔗_extractedLink0
    //gov.utah_gov.utah.67uj-wt47.0_6_1042

  def toReferences(lineages: IndexedSeq[RoleLineageWithID],trainTimeEnd:LocalDate) = {
    val sortedByID = lineages
      .sortBy(_.id)
      .zipWithIndex
    val asMap = sortedByID.map(t => (t._2, t._1)).toMap
    val roleset = Roleset(sortedByID.map(_._1.id),asMap).toNonCaseClass(trainTimeEnd)
    asMap.toIndexedSeq.sortBy(_._1).map(t => RoleReference(roleset,t._1))
  }

  def printTabularEventLineageString(vertices:collection.Seq[RoleLineageWithID]) = {
    println(getTabularEventLineageString(vertices))
  }

  def getTabularEventLineageString(vertices:collection.Seq[RoleLineageWithID]):String = {
    val allDates = vertices.flatMap(_.roleLineage.lineage.keySet).sortBy(_.toEpochDay).toSet.toIndexedSeq.sorted
    val header = Seq("") ++ allDates
    val cellsAll = vertices.map(v => {
      Seq(v.id) ++ allDates.map(t => v.roleLineage.toRoleLineage.valueAt(t)).map(v => if(RoleLineage.isWildcard(v)) "_" else v)
    }).toSeq
    TableFormatter.format(Seq(header) ++ cellsAll)
  }


  val digitRegex = "[0-9]+"

  def getTransitionHistogramForTFIDFFromVertices(vertices:Seq[RoleLineageWithID], granularityInDays:Int) :Map[ValueTransition,Int] = {
    val allTransitions = vertices
      .flatMap( (v:RoleLineageWithID) => {
        val transitions = v.roleLineage.toRoleLineage.getValueTransitionSet(true,granularityInDays).toSeq
        transitions
      })
    allTransitions
      .groupBy(identity)
      .map(t => (t._1,t._2.size))
  }

}
