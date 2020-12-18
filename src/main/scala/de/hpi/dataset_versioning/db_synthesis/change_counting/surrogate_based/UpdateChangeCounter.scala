package de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based

import java.time.LocalDate
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch, SurrogateBasedTemporalRow}
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

import scala.collection.mutable

class UpdateChangeCounter() extends FieldChangeCounter{

  def countChangesForValueLineage[A](vl: mutable.TreeMap[LocalDate, A],isWildcard : (A => Boolean)):(Int,Int) = {
    val it = vl.valuesIterator
    var prevprev:Option[A] = None
    var prev:Option[A] = None
    var cur:Option[A] = None
    var curMinChangeCount = 0
    var curMaxChangeCount = 0
    var hadFirstNonWildcardInsert = false
    var hadFirstElem = false
    while(it.hasNext){
      val newElem = Some(it.next())
      //update pointers
      prevprev = prev
      prev = cur
      cur = newElem
      //min change count:
      if(!isWildcard(newElem.get)){
        if(!hadFirstNonWildcardInsert){
          //do not count first insert
          hadFirstNonWildcardInsert = true
        } else{
          //we count this, only if prev was not WC and prevprev the same as this (avoids Wildcard to element Ping-Pong)
          assert(prev.isDefined)
          if(isWildcard(prev.get) && prevprev.isDefined && prevprev.get == cur.get){
            //skip this
          } else{
            curMinChangeCount+=1
          }
        }
      }
      //max change count:
      if(!hadFirstElem){
        hadFirstElem = true
      } else {
        if(cur.get!=prev.get || isWildcard(cur.get) || isWildcard(prev.get)){
          curMaxChangeCount +=1
        }
      }
    }
    (curMinChangeCount,curMaxChangeCount)
  }

  def countFieldChanges(r: SurrogateBasedTemporalRow) = {
    val vl = r.value.getValueLineage
    countChangesForValueLineage[Any](vl,ValueLineage.isWildcard)
  }

  def countFieldChangesSimple[A](tuple: collection.Seq[TemporalFieldTrait[A]]) = {
    assert(tuple.size==1)
    val vl = tuple(0).getValueLineage
    countChangesForValueLineage[A](vl,tuple(0).isWildcard)
  }

  def countChanges(table:SurrogateBasedSynthesizedTemporalDatabaseTableAssociation) = {
    sumChangeRanges(table.surrogateBasedTemporalRows.map(r => countFieldChanges(r)))
  }

  def countChanges(table:SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch) = {
    sumChangeRanges(table.surrogateBasedTemporalRowSketches.map(r => countFieldChangesSimple(Seq(r.valueSketch))))
  }

  override def countChanges(table:TemporalTable) = {
    sumChangeRanges(table.rows.flatMap(_.fields.map(vl => countChangesForValueLineage(vl.lineage,ValueLineage.isWildcard))))
  }

  override def name: String = "UpdateChangeCounter"

  override def countColumnChanges[A](tc: TemporalColumnTrait[A]): (Int,Int) = {
    val minMaxScores = tc.fieldLineages
      .map(_.countChanges(this))
    sumChangeRanges(minMaxScores)
  }

  def sumChangeRanges[A](minMaxScores: collection.Iterable[(Int, Int)]) = {
    if (minMaxScores.size == 1)
      minMaxScores.head
    else
      minMaxScores.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
  }

  override def countFieldChanges[A](f: TemporalFieldTrait[A]): (Int, Int) =  countChangesForValueLineage[A](f.getValueLineage,f.isWildcard)


}
