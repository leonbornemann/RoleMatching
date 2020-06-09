package de.hpi.dataset_versioning.data.change

import java.time.LocalDate

import com.google.gson.{JsonElement, JsonNull}
import de.hpi.dataset_versioning.data.diff.semantic.{DiffSimilarity}
import de.hpi.dataset_versioning.data.simplified.{Attribute, RelationalDataset, RelationalDatasetRow}
import de.hpi.dataset_versioning.util.TableFormatter

import scala.collection.mutable

class ChangeCube(val v1:RelationalDataset, val v2:RelationalDataset,
                 val inserts:mutable.ArrayBuffer[Change] = mutable.ArrayBuffer[Change](),
                 val deletes:mutable.ArrayBuffer[Change] = mutable.ArrayBuffer[Change](),
                 val updates:mutable.ArrayBuffer[Change] = mutable.ArrayBuffer[Change]()) {

  def colIdToAppearance = {
    val attrByIDOld = v1.getAttributesByID
    val attrByIDNew = v2.getAttributesByID
    attrByIDOld.keySet.union(attrByIDNew.keySet).map(id => {
      (id,(attrByIDOld.get(id),attrByIDNew.get(id)))
    }).toMap
  }

  def insertedColumns = {
    val attrByID = v2.getAttributesByID
    v2.attributes.map(_.id).toSet.diff(v1.attributes.map(_.id).toSet)
      .map(id => attrByID(id))
  }
  def columnDeletes = {
    val attrByID = v1.getAttributesByID
    v1.attributes.map(_.id).toSet.diff(v2.attributes.map(_.id).toSet)
      .map(id => attrByID(id))
  }

  def addChangesForMatchedTuples(changeTimestamp: LocalDate, prev: RelationalDatasetRow, current: RelationalDatasetRow, attributesPrev: collection.IndexedSeq[Attribute], attributesCurrent: collection.IndexedSeq[Attribute]) = {
    val prevFieldsByCOlID = getFieldsByColID(prev, attributesPrev)
    val curFieldsByCOlID = getFieldsByColID(current,attributesCurrent)
    curFieldsByCOlID.keySet.union(prevFieldsByCOlID.keySet).foreach(colID => {
      val oldValue = prevFieldsByCOlID.getOrElse(colID,null)
      val newValue = curFieldsByCOlID.getOrElse(colID,null)
      if(newValue==null)
        deletes += Change(changeTimestamp,prev.id,colID,oldValue,None)
      else if(oldValue == null)
        inserts += Change(changeTimestamp,current.id,colID,None,newValue)
      else if(oldValue != newValue)
        updates += Change(changeTimestamp,prev.id,colID,oldValue,newValue)
    })
  }


  private def getFieldsByColID(current: RelationalDatasetRow, attributesCurrent: collection.IndexedSeq[Attribute]) = {
    attributesCurrent.zip(current.fields).map { case (a, f) => (a.id, f) }
      .toMap
  }

  def addDelete(version: LocalDate, row: RelationalDatasetRow, attributes: collection.IndexedSeq[Attribute]) = {
    assert(row.fields.size == attributes.size)
    for( i <- (0 until row.fields.size)){
      deletes += Change(version,row.id,attributes(i).id,row.fields(i),None)
    }
  }

  def addInserts(version:LocalDate, row: RelationalDatasetRow, attributes: collection.IndexedSeq[Attribute]) = {
    assert(row.fields.size == attributes.size)
    for( i <- (0 until row.fields.size)){
      inserts += Change(version,row.id,attributes(i).id,None,row.fields(i))
    }
  }

  def generalizedJaccardDistance(a: Seq[String], b: Seq[String]) = {
    a.intersect(b).size / (a.size + b.size).toDouble
  }

  def multiSetContainment[A](a: collection.Seq[A], b: collection.Seq[A]) = {
    if(Math.max(a.size,b.size)==0)
      0.0
    else {
      a.intersect(b).size / Math.max(a.size,b.size).toDouble
    }
  }

  def diffSchemaSimilarity(other: ChangeCube) = {
    val insertsA = insertedColumns.map(_.name).toSeq
    val insertsB = other.insertedColumns.map(_.name).toSeq
    multiSetContainment(insertsA,insertsB)
  }

  def calculateDiffSimilarity(other:ChangeCube) = {
    val myUpdates = updates.map(u => (u.prevValue,u.newValue))
    val otherUpdates = other.updates.map(u => (u.prevValue,u.newValue))
    val myNewValues = inserts.map(_.newValue) ++ updates.map(_.newValue)
    val otherNewValues = other.inserts.map(_.newValue)  ++ other.updates.map(_.newValue)
    val myDeletedValues = deletes.map(_.prevValue) ++ updates.map(_.prevValue)
    val otherDeletedValues = other.deletes.map(_.prevValue) ++ other.updates.map(_.prevValue)
    DiffSimilarity(diffSchemaSimilarity(other),
      multiSetContainment(myNewValues,otherNewValues),
      multiSetContainment(myDeletedValues,otherDeletedValues),
      multiSetContainment(myUpdates,otherUpdates),
      myNewValues.toSet.intersect(otherNewValues.toSet),
      myDeletedValues.toSet.intersect(otherDeletedValues.toSet),
      myUpdates.toSet.intersect(otherUpdates.toSet)
    )
  }

  /*
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
  }*/

  def entireRowDeletes  = {
    val prevColset = v1.attributes.map(_.id).toSet
    val byRow = deletes.groupBy(_.e)
      .filter(_._2.map(_.pID)==columnDeletes)
    byRow.keySet
  }

}
object ChangeCube {

  def fromDatasetVersions(v1:RelationalDataset,v2:RelationalDataset) = {
    //change quadruples:
    val changeCube = new ChangeCube(v1,v2)
    val v2RowsByID = getRowsByID(v2)
    val v1RowsByID = getRowsByID(v1)
    val changeTimestamp = v2.version
    v2RowsByID.keySet.union(v1RowsByID.keySet).foreach(rID =>{
      val prev = v1RowsByID.getOrElse(rID,null)
      val current = v2RowsByID.getOrElse(rID,null)
      if(prev==null){
        //we have an insert!
        changeCube.addInserts(changeTimestamp,current,v2.attributes)
      } else if (current == null){
        changeCube.addDelete(changeTimestamp,prev,v1.attributes)
      } else{
        changeCube.addChangesForMatchedTuples(changeTimestamp,prev,current,v1.attributes,v2.attributes)
      }
    })
    changeCube
  }

  private def getRowsByID(v2: RelationalDataset) = {
    v2.rows
      .map(r => (r.id, r))
      .toMap
  }
}
