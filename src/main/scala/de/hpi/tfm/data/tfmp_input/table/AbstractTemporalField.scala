package de.hpi.tfm.data.tfmp_input.table

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{CommonPointOfInterestIterator, FactLineage}
import de.hpi.tfm.evaluation.wikipediaStyle.RemainsValidVariant
import de.hpi.tfm.evaluation.wikipediaStyle.RemainsValidVariant.RemainsValidVariant
import de.hpi.tfm.fact_merging.config.UpdateChangeCounter
import org.joda.time.Duration

import java.time.LocalDate
import scala.collection.mutable

@SerialVersionUID(3L)
abstract class AbstractTemporalField[A] extends TemporalFieldTrait[A] {


  override def isConsistentWith(v2: TemporalFieldTrait[A], minTimePercentage: Double) = {
    val commonPointOfInterestIterator = new CommonPointOfInterestIterator[A](this,v2)
    var validDuration = 0
    var invalidDuration = 0
    commonPointOfInterestIterator
      .foreach(cp => {
        var durationToAdd = (cp.pointInTime.toEpochDay - cp.prevPointInTime.toEpochDay).toInt
        if(valuesAreCompatible(cp.curValueA,cp.curValueB)){
          validDuration +=durationToAdd
        } else {
          invalidDuration +=durationToAdd
        }
      })
    validDuration / (validDuration+invalidDuration).toDouble > minTimePercentage
  }

  override def countChanges(changeCounter: UpdateChangeCounter): (Int,Int) = {
    changeCounter.countFieldChanges(this)
  }

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

  def valuesAreCompatible(myValue: A, otherValue: A,variant:RemainsValidVariant = RemainsValidVariant.STRICT):Boolean

  def getCompatibleValue(myValue: A, otherValue: A): A

  def getOverlapInterval(a: (TimeInterval, A), b: (TimeInterval, A)): (TimeInterval, A) = {
    assert(a._1.begin==b._1.begin)
    assert(valuesAreCompatible(a._2,b._2))
    val earliestEnd = Seq(a._1.endOrMax,b._1.endOrMax).minBy(_.toEpochDay)
    val endTime = if(earliestEnd==LocalDate.MAX) None else Some(earliestEnd)
    (TimeInterval(a._1.begin,endTime),getCompatibleValue(a._2,b._2))
  }

  def fromValueLineage[V<:TemporalFieldTrait[A]](lineage: FactLineage):V

  def fromTimestampToValue[V<:TemporalFieldTrait[A]](asTree: mutable.TreeMap[LocalDate, A]):V

  override def tryMergeWithConsistent[V <: TemporalFieldTrait[A]](other: V,variant:RemainsValidVariant = RemainsValidVariant.STRICT): Option[V] = {
    val myLineage = scala.collection.mutable.ArrayBuffer() ++ this.toIntervalRepresentation
    val otherLineage = scala.collection.mutable.ArrayBuffer() ++ other.toIntervalRepresentation.toBuffer
    if(myLineage.isEmpty){
      return if(otherLineage.isEmpty) Some(fromValueLineage[V](FactLineage())) else None
    } else if(otherLineage.isEmpty){
      return if(myLineage.isEmpty) Some(fromValueLineage[V](FactLineage())) else None
    }
    val newLineage = mutable.ArrayBuffer[(TimeInterval,A)]()
    var myIndex = 0
    var otherIndex = 0
    var incompatible = false
    while((myIndex < myLineage.size || otherIndex < otherLineage.size ) && !incompatible) {
      assert(myIndex < myLineage.size && otherIndex < otherLineage.size)
      val (myInterval,myValue) = myLineage(myIndex)
      val (otherInterval,otherValue) = otherLineage(otherIndex)
      assert(myInterval.begin == otherInterval.begin)
      var toAppend:(TimeInterval,A) = null
      if(myInterval==otherInterval){
        if(!valuesAreCompatible(myValue,otherValue,variant)){
          incompatible=true
        } else {
          toAppend = (myInterval, getCompatibleValue(myValue, otherValue))
          myIndex += 1
          otherIndex += 1
        }
      } else if(myInterval<otherInterval){
        if(!valuesAreCompatible(myLineage(myIndex)._2,otherLineage(otherIndex)._2,variant)){
          incompatible = true
        } else {
          toAppend = getOverlapInterval(myLineage(myIndex), otherLineage(otherIndex))
          //replace old interval with newer interval with begin set to myInterval.end+1
          otherLineage(otherIndex) = (TimeInterval(myInterval.end.get.plusDays(1), otherInterval.`end`), otherValue)
          myIndex += 1
        }
      } else{
        assert(otherInterval<myInterval)
        if(!valuesAreCompatible(myLineage(myIndex)._2,otherLineage(otherIndex)._2,variant)){
          incompatible = true
        } else {
          toAppend = getOverlapInterval(myLineage(myIndex), otherLineage(otherIndex))
          myLineage(myIndex) = (TimeInterval(otherInterval.end.get.plusDays(1), myInterval.`end`), myValue)
          otherIndex += 1
        }
      }
      if(!incompatible) {
        if (!newLineage.isEmpty && newLineage.last._2 == toAppend._2) {
          //we replace the old interval by a longer one
          newLineage(newLineage.size - 1) = (TimeInterval(newLineage.last._1.begin, toAppend._1.`end`), toAppend._2)
        } else {
          //we simply append the new interval:
          newLineage += toAppend
        }
      }
    }
    if(incompatible)
      None
    else {
      val asTree = mutable.TreeMap[LocalDate, A]() ++ newLineage.map(t => (t._1.begin, t._2))
      Some(fromTimestampToValue[V](asTree))
    }
  }

  override def mergeWithConsistent[V<:TemporalFieldTrait[A]](other: V):V = {
    tryMergeWithConsistent(other).get
  }

  override def append[V<:TemporalFieldTrait[A]](y: V): V = {
    assert(lastTimestamp.isBefore(y.firstTimestamp))
    fromTimestampToValue(this.getValueLineage ++ y.getValueLineage)
  }

}
object AbstractTemporalField{


  def multipleEventWeightScore[A](tr1: TupleReference[A], tr2: TupleReference[A]) = {
    tr1.getDataTuple.head.newScore(tr2.getDataTuple.head)
  }


  def ENTROPY_REDUCTION[A](tr1: TupleReference[A], tr2: TupleReference[A]) = {
    ENTROPY_REDUCTION_SET(Set(tr1,tr2))
  }

  def ENTROPY_REDUCTION_SET[A](references: Set[TupleReference[A]]) = {
    val asFields = references.map(_.getDataTuple.head)
    ENTROPY_REDUCTION_SET_FIELD(asFields)
  }

  def ENTROPY_REDUCTION_SET_FIELD[A](fields:Set[TemporalFieldTrait[A]]) = {
    val merged = fields.toIndexedSeq
      .reduce((a,b) => a.mergeWithConsistent(b)).getEntropy()
    val prior = fields.toIndexedSeq.map(_.getEntropy()).sum
    if(prior.isNaN || merged.isNaN)
      println("What? - we found NaN")
    prior - merged
  }

  def mergeAll[A](refs:Seq[TupleReference[A]]):TemporalFieldTrait[A] = {
    if(refs.size==1)
      refs.head.getDataTuple.head
    else {
      val toMerge = refs.tail.map(tr => tr.getDataTuple)
      assert(toMerge.head.size == 1)
      var res = refs.head.getDataTuple.head
      (1 until toMerge.size).foreach(i => {
        res = res.mergeWithConsistent(toMerge(i).head)
      })
      res
    }
  }
}
