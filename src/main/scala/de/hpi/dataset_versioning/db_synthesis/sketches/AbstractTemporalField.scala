package de.hpi.dataset_versioning.db_synthesis.sketches

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TimeInterval
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.Variant2Sketch.{byteArraySliceToInt, byteToTimestamp}

import scala.collection.mutable
import scala.collection.mutable.HashMap

@SerialVersionUID(3L)
abstract class AbstractTemporalField[A] extends TemporalFieldTrait[A] {

  override def toIntervalRepresentation:mutable.TreeMap[TimeInterval,A] = {
    val asLineage = getValueLineage.toIndexedSeq
    mutable.TreeMap[TimeInterval,A]() ++ (0 until asLineage.size).map( i=> {
      val (ts,value) = asLineage(i)
      if(i==asLineage.size-1)
        (TimeInterval(ts,None),value)
      else
        (TimeInterval(ts,Some(asLineage(i+1)._1.minusDays(1))),value)
    })
  }

  def valuesAreCompatible(myValue: A, otherValue: A):Boolean

  def getCompatibleValue(myValue: A, otherValue: A): A

  def getOverlapInterval(a: (TimeInterval, A), b: (TimeInterval, A)): (TimeInterval, A) = {
    assert(a._1.begin==b._1.begin)
    assert(valuesAreCompatible(a._2,b._2))
    val earliestEnd = Seq(a._1.endOrMax,b._1.endOrMax).minBy(_.toEpochDay)
    val endTime = if(earliestEnd==LocalDate.MAX) None else Some(earliestEnd)
    (TimeInterval(a._1.begin,endTime),getCompatibleValue(a._2,b._2))
  }

  def fromValueLineage[V<:TemporalFieldTrait[A]](lineage: ValueLineage):V

  def fromTimestampToValue[V<:TemporalFieldTrait[A]](asTree: mutable.TreeMap[LocalDate, A]):V

  override def mergeWithConsistent[V<:TemporalFieldTrait[A]](other: V):V = {
    val myLineage = this.toIntervalRepresentation.toBuffer
    val otherLineage = other.toIntervalRepresentation.toBuffer
    if(myLineage.isEmpty){
      assert(otherLineage.isEmpty)
      fromValueLineage[V](ValueLineage())
    } else if(otherLineage.isEmpty){
      assert(myLineage.isEmpty)
      fromValueLineage[V](ValueLineage())
    }
    val newLineage = mutable.ArrayBuffer[(TimeInterval,A)]()
    var myIndex = 0
    var otherIndex = 0
    while(myIndex < myLineage.size || otherIndex < otherLineage.size) {
      assert(myIndex < myLineage.size && otherIndex < otherLineage.size)
      val (myInterval,myValue) = myLineage(myIndex)
      val (otherInterval,otherValue) = otherLineage(otherIndex)
      if(myInterval.begin != otherInterval.begin){
        println()
      }
      assert(myInterval.begin == otherInterval.begin)
      var toAppend:(TimeInterval,A) = null
      if(myInterval==otherInterval){
        assert(valuesAreCompatible(myValue,otherValue))
        toAppend = (myInterval,getCompatibleValue(myValue,otherValue))
        myIndex+=1
        otherIndex+=1
      } else if(myInterval<otherInterval){
        toAppend = getOverlapInterval(myLineage(myIndex),otherLineage(otherIndex))
        //replace old interval with newer interval with begin set to myInterval.end+1
        otherLineage(otherIndex) = (TimeInterval(myInterval.end.get,otherInterval.`end`),otherValue)
        myIndex+=1
      } else{
        assert(otherInterval<myInterval)
        toAppend = getOverlapInterval(myLineage(myIndex),otherLineage(otherIndex))
        myLineage(myIndex) = (TimeInterval(otherInterval.end.get,myInterval.`end`),myValue)
        otherIndex+=1
      }
      if(!newLineage.isEmpty && newLineage.last._2==toAppend._2){
        //we replace the old interval by a longer one
        newLineage(newLineage.size-1) = (TimeInterval(newLineage.last._1.begin,toAppend._1.`end`),toAppend._2)
      } else{
        //we simply append the new interval:
        newLineage += toAppend
      }
    }
    val asTree = mutable.TreeMap[LocalDate,A]() ++ newLineage.map(t => (t._1.begin,t._2))
    fromTimestampToValue[V](asTree)
  }

  override def append[V<:TemporalFieldTrait[A]](y: V): V = {
    assert(lastTimestamp.isBefore(y.firstTimestamp))
    fromTimestampToValue(this.getValueLineage ++ y.getValueLineage)
  }

  def valuesInInterval(ti: TimeInterval): IterableOnce[(TimeInterval,A)]

  //gets the hash values at the specified time-intervals, substituting missing values with the hash-value of ReservedChangeValues.NOT_EXISTANT_ROW
  override def valuesAt(timeToExtract: TimeIntervalSequence): Map[TimeInterval, A] = {
    val a = timeToExtract.sortedTimeIntervals.flatMap(ti => {
      valuesInInterval(ti)
    }).toMap
    a
  }


}
