package de.hpi.dataset_versioning.data.change.temporal_tables

import java.time.LocalDate

import scala.collection.mutable.ArrayBuffer

//begin inclusive, end exclusive
case class TimeInterval(begin:LocalDate,private val constructedEnd:Option[LocalDate]) extends Ordered[TimeInterval]{

  val end = if(constructedEnd.isDefined && constructedEnd.get == LocalDate.MAX) None else constructedEnd // convenience constructor, so that we can specify LocalDate.Max instead of None

  assert(!end.isDefined || end.get!=LocalDate.MAX)

  def intersect(toMerge: TimeInterval) = {
    val newBegin = Seq(toMerge.begin,this.begin).max
    val newEnd = if(toMerge.`end`.isEmpty && this.`end`.isEmpty) None else Some(Seq(toMerge.endOrMax,this.endOrMax).min)
    if(newEnd.isDefined && newBegin.isAfter(newEnd.get))
      None
    else
      Some(TimeInterval(newBegin,newEnd))
  }


  def mergeWithOverlapping(toMerge: TimeInterval) = {
    val newEnd = if(toMerge.`end`.isEmpty || this.`end`.isEmpty) None else Some(Seq(toMerge.endOrMax,this.endOrMax).max)
    if(begin == toMerge.begin){
      TimeInterval(begin,newEnd)
    } else if(begin.isBefore(toMerge.begin)){
      assert(toMerge.begin.isBefore(this.endOrMax) || toMerge.begin == this.endOrMax)
      TimeInterval(begin,newEnd)
    } else{
      assert(this.begin.isBefore(toMerge.endOrMax) || this.begin == toMerge.endOrMax)
      TimeInterval(toMerge.begin,newEnd)
    }
  }

  override def <(other: TimeInterval) = begin.isBefore(other.begin) ||
    (begin==other.begin && endOrMax.isBefore(other.endOrMax))

  override def <=(other:TimeInterval) = this < other || this == other

  def subIntervalOf(other: TimeInterval) = {
    !begin.isBefore(other.begin) && !end.getOrElse(LocalDate.MAX).isAfter(other.end.getOrElse(LocalDate.MAX))
  }

  def endOrMax = end.getOrElse(LocalDate.MAX)

  override def compare(that: TimeInterval): Int = {
    if(this==that) 0
    else if(this < that) -1
    else 1
  }
}

object TimeInterval {

  def notIncludedIN(sortedIntervalsA: ArrayBuffer[TimeInterval], sortedIntervalsB: ArrayBuffer[TimeInterval]): Boolean = {
    val itA = sortedIntervalsA.iterator
    val itB = sortedIntervalsB.iterator
    var included = true
    var curElemB = itB.next()
    while(included && itA.hasNext){
      var curElemA = itA.next()
      while(!curElemA.begin.isBefore(curElemB.endOrMax) && itB.hasNext){
        //the interval is too early
        curElemB = itB.next()
      }
      included = curElemA.subIntervalOf(curElemB)
    }
    included
  }
}
