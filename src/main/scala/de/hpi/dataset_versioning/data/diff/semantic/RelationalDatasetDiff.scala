package de.hpi.dataset_versioning.data.diff.semantic

import com.google.gson.{JsonElement, JsonNull}
import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.LoadedRelationalDataset
import de.hpi.dataset_versioning.util.TableFormatter

import scala.collection.mutable

class RelationalDatasetDiff(val unchanged: mutable.HashSet[Set[(String, JsonElement)]],
                            val inserts: mutable.HashSet[Set[(String, JsonElement)]],
                            val deletes: mutable.HashSet[Set[(String, JsonElement)]],
                            val updates:mutable.HashMap[Set[(String, JsonElement)],Set[(String, JsonElement)]],
                            var schemaChange: SchemaChange = new SchemaChange,
                            var incomplete:Boolean=false) extends StrictLogging{

  def calculateChangeMetrics() = {
    val unchangedMap = unchanged.map(_.toMap)
    val insertsMap = inserts.map(_.toMap)
    val deletesMap = deletes.map(_.toMap)
    val updatesMap = updates.map(t => (t._1.toMap,t._2.toMap))
    //val rcs = unchangedMap.map()
    //val metrics = new DatasetChangeMetrics()
    //TODO:later
  }

  def generalizedJaccardDistance(a: Seq[String], b: Seq[String]) = {
    a.intersect(b).size / (a.size + b.size).toDouble
  }

  def multiSetContainment[A](a: Seq[A], b: Seq[A]) = {
    if(Math.max(a.size,b.size)==0)
      0.0
    else {
      a.intersect(b).size / Math.max(a.size,b.size).toDouble
    }
  }

  def diffSchemaSimilarity(other: RelationalDatasetDiff) = {
    val insertsA = schemaChange.columnInsert.getOrElse(Seq())
    val insertsB = other.schemaChange.columnInsert.getOrElse(Seq())
    multiSetContainment(insertsA,insertsB)
  }

  private def getOldToNewValueUpdates = {
    updates.toSeq.flatMap { case (t1, t2) => {
      val oldFieldMap = t1.toMap
      val newFieldMap = t1.toMap
      //let's assume for now that there was no schema change
      val fieldNameIntersection = oldFieldMap.keySet.union(newFieldMap.keySet)
      val valueUpdates = fieldNameIntersection.toSeq.map(fieldKey => {
        (oldFieldMap.getOrElse(fieldKey, JsonNull.INSTANCE), newFieldMap.getOrElse(fieldKey, JsonNull.INSTANCE))
      })
          .filter(t => t._1!=t._2)
      valueUpdates
    }
    }
  }

  def calculateDiffSimilarity(other:RelationalDatasetDiff) = {
    val myUpdates = getOldToNewValueUpdates
    val otherUpdates = other.getOldToNewValueUpdates
    val myNewValues = inserts.toSeq.flatMap(_.toSeq.map(_._2)) ++ myUpdates.map(_._2)
    val otherNewValues = other.inserts.toSeq.flatMap(_.toSeq.map(_._2)) ++ otherUpdates.map(_._2)
    val myDeletedValues = deletes.toSeq.flatMap(_.toSeq.map(_._2)) ++ myUpdates.map(_._1)
    val otherDeletedValues = other.deletes.toSeq.flatMap(_.toSeq.map(_._2)) ++ otherUpdates.map(_._1)
    DiffSimilarity(diffSchemaSimilarity(other),
      multiSetContainment(myNewValues,otherNewValues),
      multiSetContainment(myDeletedValues,otherDeletedValues),
      multiSetContainment(myUpdates,otherUpdates),
      myNewValues.toSet.intersect(otherNewValues.toSet),
      myDeletedValues.toSet.intersect(otherDeletedValues.toSet)
    )
  }

  def getAsTableString(rows: scala.collection.Set[Set[(String, JsonElement)]]) = {
    if(rows.isEmpty)
      ""
    else {
      val schema = rows.head.map(_._1).toIndexedSeq.sorted
      val content = rows.map(_.toIndexedSeq.sortBy(_._1).map(_._2)).toIndexedSeq
      TableFormatter.format(Seq(schema) ++ content)
    }
  }

  def getUpdatesAsTableString() = {
    if(updates.isEmpty)
      ""
    else{
      val schemaLeft = updates.keySet.head.map(_._1).toIndexedSeq.sorted
      val schemaRight = updates.values.head.map(_._1).toIndexedSeq.sorted
      val schema = schemaLeft ++ Seq("-->") ++schemaRight
      val content = updates.map{case (l,r) => {
        val left = l.toIndexedSeq.sortBy(_._1).map(_._2)
        val right = r.toIndexedSeq.sortBy(_._1).map(_._2)
        left ++ Seq("-->") ++ right
      }}.toIndexedSeq
      TableFormatter.format(Seq(schema) ++ content)
    }
  }

  def print() = {
    logger.debug( //TODO: print updates as well
      s"""
         |-----------Schema Changes:--------------
         |${schemaChange.getAsPrintString}
         |----------------------------------------
         |-----------Data Changes-----------------
         |Inserts:
         |${getAsTableString(inserts)}
         |Deletes:
         |${getAsTableString(deletes)}
         |Updates:
         |${getUpdatesAsTableString()}
         |""".stripMargin
    )
  }
}

object RelationalDatasetDiff extends StrictLogging {

  def getColumnMapping(from: LoadedRelationalDataset, to: LoadedRelationalDataset, predefinedEdges: Option[Set[(String, String)]]) = {
    var abort = false
    val columnMapping = mutable.HashMap[String,String]()
    val takenColumns = scala.collection.mutable.HashSet[String]()
    if(from.columnMetadata.isEmpty) from.calculateColumnMetadata()
    if(to.columnMetadata.isEmpty) to.calculateColumnMetadata()
    //if we have edges from the joinability graph we can use those:
    if(predefinedEdges.isDefined){
      val map = predefinedEdges.get.groupBy(_._1)
        .toSeq
        .sortBy(_._2.size)
      val a = map.iterator
      while(a.hasNext && !abort){
        var (srcCol,edges) = a.next()
        val candidateMatches = edges.map(_._2)
          .filter(!takenColumns.contains(_))
        if(candidateMatches.size==0)
          abort = true
        else if(edges.size==1){
          takenColumns += candidateMatches.head
          columnMapping(srcCol) = candidateMatches.head
        } else{
          takenColumns += candidateMatches.head
          columnMapping(srcCol) = candidateMatches.head
          logger.debug("encountered difficult column matching case")
        }
      }
    }
    if(abort){
      None
    } else{
      //try to match the rest of the columns:
      val freeInProjected = from.colNames.toSet.diff(columnMapping.keySet)
        .map(s => (s,from.columnMetadata(s).hash))
        .toMap
      if(freeInProjected.isEmpty)
        Some(columnMapping)
      else{
        val freeInOriginal = to.colNames.toSet.diff(columnMapping.values.toSet) //TODO: make the map the other way around here! - saves code later
          .map(s => (s,to.columnMetadata(s).hash))
          .toMap
        //invert the maps:
        val projectedHashToColname = freeInProjected.groupBy(_._2).mapValues(_.keys)
        val originalHashToColname = freeInOriginal.groupBy(_._2).mapValues(_.keys)
        val it = projectedHashToColname.iterator
        while(it.hasNext && !abort){
          val (projectedHash,colNamesProjected) = it.next()
          if(!originalHashToColname.contains(projectedHash)){
            abort = true
          } else {
            val matchingColNames = originalHashToColname(projectedHash).toSeq
              .filter(!takenColumns.contains(_))
            if (colNamesProjected.size == matchingColNames.size) {
              columnMapping ++= colNamesProjected.zip(matchingColNames)
              takenColumns ++= matchingColNames
            } else {
              abort = true
            }
          }
        }
        if(abort) None
        else Some(columnMapping)
      }
    }
  }

}
