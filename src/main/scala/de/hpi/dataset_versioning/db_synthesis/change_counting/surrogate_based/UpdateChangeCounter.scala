package de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch, SurrogateBasedTemporalRow}
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

import scala.collection.mutable

class UpdateChangeCounter() {

  def countChangesForValueLineage[A](vl: mutable.TreeMap[LocalDate, A],isWildcard : (A => Boolean)) = {
    val it = vl.valuesIterator
    var prevprev:Option[A] = None
    var prev:Option[A] = None
    var cur:Option[A] = None
    var curChangeCount = 0
    var hadFirstInsert = false
    while(it.hasNext){
      val newElem = Some(it.next())
      //update pointers
      prevprev = prev
      prev = cur
      cur = newElem
      if(!isWildcard(newElem.get)){
        if(!hadFirstInsert){
          //do not count first insert
          hadFirstInsert = true
        } else{
          //we count this, only if prev was not WC and prevprev the same as this (avoids Wildcard to element Ping-Pong)
          assert(prev.isDefined)
          if(isWildcard(prev.get) && prevprev.isDefined && prevprev.get == cur.get){
            //skip this
          } else{
            curChangeCount+=1
          }
        }
      }
    }
    curChangeCount
  }

  def countFieldChanges(r: SurrogateBasedTemporalRow) = {
    val vl = r.value.getValueLineage
    countChangesForValueLineage[Any](vl,ValueLineage.isWildcard)
  }

  def countFieldChanges[A](tuple: collection.Seq[TemporalFieldTrait[A]]) = {
    assert(tuple.size==1)
    val vl = tuple(0).getValueLineage
    countChangesForValueLineage[A](vl,tuple(0).isWildcard)
  }

  def countChanges(table:SurrogateBasedSynthesizedTemporalDatabaseTableAssociation) = {
    table.surrogateBasedTemporalRows.map(r => countFieldChanges(r)).sum
  }

  def countChanges(table:SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch) = {
    table.surrogateBasedTemporalRowSketches.map(r => countFieldChanges(Seq(r.valueSketch))).sum
  }

  def countChanges(table:TemporalTable) = {
    table.rows.flatMap(_.fields.map(vl => countChangesForValueLineage(vl.lineage,ValueLineage.isWildcard).toLong)).sum
  }

}
