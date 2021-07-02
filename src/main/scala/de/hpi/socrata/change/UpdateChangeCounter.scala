package de.hpi.socrata.change

import de.hpi.socrata.change.temporal_tables.TemporalTable
import de.hpi.socrata.tfmp_input.table.nonSketch.{FactLineage, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow}
import de.hpi.socrata.tfmp_input.table.sketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
import de.hpi.socrata.tfmp_input.table.{TemporalDatabaseTableTrait, TemporalFieldTrait}

import java.time.LocalDate
import scala.collection.mutable

class UpdateChangeCounter(){

  def countChangesForValueLineage[A](vl: mutable.TreeMap[LocalDate, A],isWildcard : (A => Boolean)):(Int,Int) = {
    val it = vl.valuesIterator
    var prevNonWildcard:Option[A] = None
    var prev:Option[A] = None
    var cur:Option[A] = None
    var curMinChangeCount = 0
    var curMaxChangeCount = 0
    var hadFirstElem = false
    while(it.hasNext){
      val newElem = Some(it.next())
      //update pointers
      prev = cur
      cur = newElem
      //min change count:
      if(!isWildcard(newElem.get)){
        //we count this, only if prev was not WC and prevprev the same as this (avoids Wildcard to element Ping-Pong)
        if(!prevNonWildcard.isDefined){
          //skip
        } else if(prevNonWildcard.get!=newElem.get) {
          curMinChangeCount+=1
        }
        prevNonWildcard = newElem
      }
      //max change count:
      if(!hadFirstElem){
        hadFirstElem = true
      } else {
        if(cur.get!=prev.get || isWildcard(cur.get)){
          curMaxChangeCount +=1
        }
      }
    }
    (curMinChangeCount,curMaxChangeCount)
  }

  def countFieldChanges(r: SurrogateBasedTemporalRow) = {
    val vl = r.value.getValueLineage
    countChangesForValueLineage[Any](vl,FactLineage.isWildcard)
  }

  def countChanges[A](table: TemporalDatabaseTableTrait[A]): (Int,Int) = {
    sumChangeRanges((0 until table.nrows).map( i=> countFieldChangesSimple(table.getDataTuple(i))))
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

  def countChanges(table:TemporalTable) = {
    sumChangeRanges(table.rows.flatMap(_.fields.map(vl => countChangesForValueLineage(vl.lineage,FactLineage.isWildcard))))
  }

  def name: String = "UpdateChangeCounter"

  def sumChangeRangesAsLong[A](minMaxScoresInt: collection.Iterable[(Int, Int)]) = {
    val minMaxScores = minMaxScoresInt.map(t => (t._1.toLong,t._2.toLong))
    if (minMaxScores.size == 0)
      (0,0)
    else if (minMaxScores.size == 1)
      minMaxScores.head
    else
      minMaxScores.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
  }

  def sumChangeRanges[A](minMaxScores: collection.Iterable[(Int, Int)]) = {
    if (minMaxScores.size == 0)
      (0,0)
    else if (minMaxScores.size == 1)
      minMaxScores.head
    else
      minMaxScores.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
  }

  def countFieldChanges[A](f: TemporalFieldTrait[A]): (Int, Int) =  countChangesForValueLineage[A](f.getValueLineage,f.isWildcard)


}