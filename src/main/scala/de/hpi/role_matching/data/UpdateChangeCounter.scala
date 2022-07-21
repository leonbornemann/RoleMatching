package de.hpi.role_matching.data

import java.time.LocalDate
import scala.collection.mutable

class UpdateChangeCounter(){

  def countChangesForValueLineage(vl: mutable.TreeMap[LocalDate, Any],isWildcard : (Any => Boolean)):(Int,Int) = {
    val it = vl.valuesIterator
    var prevNonWildcard:Option[Any] = None
    var prev:Option[Any] = None
    var cur:Option[Any] = None
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

  def countFieldChangesSimple(tuple: collection.Seq[RoleLineage]) = {
    assert(tuple.size==1)
    val vl = tuple(0).getValueLineage
    countChangesForValueLineage(vl,tuple(0).isWildcard)
  }

  def name: String = "UpdateChangeCounter"

  def sumChangeRangesAsLong(minMaxScoresInt: collection.Iterable[(Int, Int)]) = {
    val minMaxScores = minMaxScoresInt.map(t => (t._1.toLong,t._2.toLong))
    if (minMaxScores.size == 0)
      (0,0)
    else if (minMaxScores.size == 1)
      minMaxScores.head
    else
      minMaxScores.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
  }

  def sumChangeRanges(minMaxScores: collection.Iterable[(Int, Int)]) = {
    if (minMaxScores.size == 0)
      (0,0)
    else if (minMaxScores.size == 1)
      minMaxScores.head
    else
      minMaxScores.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
  }

  def countFieldChanges(f: RoleLineage): (Int, Int) =  countChangesForValueLineage(f.getValueLineage,f.isWildcard)


}