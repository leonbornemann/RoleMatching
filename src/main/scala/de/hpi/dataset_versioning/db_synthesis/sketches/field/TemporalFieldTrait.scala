package de.hpi.dataset_versioning.db_synthesis.sketches.field

import de.hpi.dataset_versioning.data.change.ReservedChangeValues

import java.time.LocalDate
import de.hpi.dataset_versioning.data.change.temporal_tables.time.{TimeInterval, TimeIntervalSequence}
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.ValueTransition
import de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based.{FieldChangeCounter, UpdateChangeCounter}
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.util.MathUtil.log2

import scala.collection.mutable

trait TemporalFieldTrait[T] {

  private var entropy:Option[Double] = None

  def getEntropy(): Double = {
    if(entropy.isDefined)
      entropy.get
    else {
      entropy = Some(new EntropyComputer(this).entropy)
      entropy.get
    }
  }

  def nonWildcardValueTransitions: Set[(T, T)] = {
    val vl = getValueLineage
      .values
      .filter(!isWildcard(_))
      .toIndexedSeq
    (1 until vl.size).map(i => (vl(i-1),vl(i))).toSet
  }


  private def notWCOrEmpty(prevValue1: Option[T]): Boolean = {
    !prevValue1.isEmpty && !isWildcard(prevValue1.get)
  }

  private def getNonWCInterleavedOverlapEvidenceMultiSet(other: TemporalFieldTrait[T]) = {
    val vl1 = this.getValueLineage
    val vl2 = other.getValueLineage
    val vl1Iterator = scala.collection.mutable.Queue() ++ vl1
    val vl2Iterator = scala.collection.mutable.Queue() ++ vl2
    var isCompatible = true
    val evidenceSet = mutable.HashMap[ValueTransition,Int]()
    var curValue1 = vl1Iterator.dequeue()._2
    var curValue2 = vl2Iterator.dequeue()._2
    var prevValue1:Option[T] = None
    var prevValue2:Option[T] = None
    while((!vl1Iterator.isEmpty || !vl2Iterator.isEmpty) && isCompatible){
      if(!isWildcard(curValue1) && !isWildcard(curValue2) && curValue1!=curValue2){
        isCompatible=false
      } else {
        if(vl1Iterator.isEmpty){
          prevValue2 = Some(curValue2)
          curValue2 = vl2Iterator.dequeue()._2
        } else if(vl2Iterator.isEmpty){
          prevValue1 = Some(curValue1)
          curValue1 = vl1Iterator.dequeue()._2
        } else {
          val ts1 = vl1Iterator.head._1
          val ts2 = vl2Iterator.head._1
          if (ts1 == ts2) {
            prevValue1 = Some(curValue1)
            curValue1 = vl1Iterator.dequeue()._2
            prevValue2 = Some(curValue2)
            curValue2 = vl2Iterator.dequeue()._2
            if (!isWildcard(curValue1) && !isWildcard(curValue2) && notWCOrEmpty(prevValue1) && notWCOrEmpty(prevValue2)) {
              assert(prevValue1.get==prevValue2.get)
              assert(curValue1 == curValue2)
              val toAdd = ValueTransition(prevValue1.get, curValue1)
              val oldValue = evidenceSet.getOrElse(toAdd,0)
              evidenceSet(toAdd) = oldValue+1
            }
          } else if (ts1.isBefore(ts2)){
            prevValue1 = Some(curValue1)
            curValue1 = vl1Iterator.dequeue()._2
          } else {
            assert(ts2.isBefore(ts1))
            prevValue2 = Some(curValue2)
            curValue2 = vl2Iterator.dequeue()._2
          }
        }
      }
    }
    if(!isWildcard(curValue1) && !isWildcard(curValue2) && curValue1!=curValue2){
      isCompatible=false
    }
    if(!isCompatible)
      None
    else {
      Some(evidenceSet)
    }
  }

  private def getWCInterleavedOverlapEvidenceMultiSet(other: TemporalFieldTrait[T]) = {
    val vl1 = this.getValueLineage
    val vl2 = other.getValueLineage
    val vl1Iterator = scala.collection.mutable.Queue() ++ vl1
    val vl2Iterator = scala.collection.mutable.Queue() ++ vl2
    var isCompatible = true
    val evidenceSet = mutable.HashMap[ValueTransition,Int]()
    var curValue1 = vl1Iterator.dequeue()._2
    var curValue2 = vl2Iterator.dequeue()._2
    var prevNonWCValue1:Option[T] = None
    var prevNonWCValue2:Option[T] = None
    while((!vl1Iterator.isEmpty || !vl2Iterator.isEmpty) && isCompatible){
      if(!isWildcard(curValue1) && !isWildcard(curValue2) && curValue1!=curValue2){
        isCompatible=false
      } else {
        if(vl1Iterator.isEmpty){
          if(!isWildcard(curValue2))
            prevNonWCValue2 = Some(curValue2)
          curValue2 = vl2Iterator.dequeue()._2
        } else if(vl2Iterator.isEmpty){
          if(!isWildcard(curValue1))
            prevNonWCValue1 = Some(curValue1)
          curValue1 = vl1Iterator.dequeue()._2
        } else {
          val ts1 = vl1Iterator.head._1
          val ts2 = vl2Iterator.head._1
          if (ts1 == ts2) {
            if(!isWildcard(curValue1))
              prevNonWCValue1 = Some(curValue1)
            curValue1 = vl1Iterator.dequeue()._2
            if(!isWildcard(curValue2))
              prevNonWCValue2 = Some(curValue2)
            curValue2 = vl2Iterator.dequeue()._2
            if (!isWildcard(curValue1) && !isWildcard(curValue2) && prevNonWCValue1.isDefined && prevNonWCValue2.isDefined) {
              assert(prevNonWCValue1.get==prevNonWCValue2.get)
              assert(curValue1 == curValue2)
              if(curValue1!=prevNonWCValue1.get){
                val toAdd = ValueTransition(prevNonWCValue1.get, curValue1)
                val oldValue = evidenceSet.getOrElse(toAdd,0)
                evidenceSet(toAdd) = oldValue+1
              }
            }
          } else if (ts1.isBefore(ts2)){
            if(!isWildcard(curValue1))
              prevNonWCValue1 = Some(curValue1)
            curValue1 = vl1Iterator.dequeue()._2
          } else {
            assert(ts2.isBefore(ts1))
            if(!isWildcard(curValue2))
              prevNonWCValue2 = Some(curValue2)
            curValue2 = vl2Iterator.dequeue()._2
          }
        }
      }
    }
    if(!isWildcard(curValue1) && !isWildcard(curValue2) && curValue1!=curValue2){
      isCompatible=false
    }
    if(!isCompatible)
      None
    else {
      Some(evidenceSet)
    }
  }

  //=GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS
  def getOverlapEvidenceMultiSet(other: TemporalFieldTrait[T]):collection.Map[ValueTransition,Int] = getOverlapEvidenceMultiSet(other,GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS)
  def getOverlapEvidenceCount(other: TemporalFieldTrait[T]):Int = getOverlapEvidenceCount(other,GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS)


  def getOverlapEvidenceMultiSet(other: TemporalFieldTrait[T],allowInterleavedWildcards:Boolean) = {
    if(!allowInterleavedWildcards) {
      val res = getNonWCInterleavedOverlapEvidenceMultiSet(other)
      res.get
    }
    else {
      val res = getWCInterleavedOverlapEvidenceMultiSet(other)
      res.get
    }
  }

  def getOverlapEvidenceCount(other: TemporalFieldTrait[T], allowInterleavedWildcards:Boolean) = {
    if(!allowInterleavedWildcards){
      val multiSet = getNonWCInterleavedOverlapEvidenceMultiSet(other)
      if(multiSet.isDefined) multiSet.get.values.sum else -1
    } else {
      val multiSet = getWCInterleavedOverlapEvidenceMultiSet(other)
      if(multiSet.isDefined) multiSet.get.values.sum else -1
    }
  }

  def valueAt(ts: LocalDate): T

  def allTimestamps: Iterable[LocalDate] = getValueLineage.keySet

  def allNonWildcardTimestamps: Iterable[LocalDate] = {
    getValueLineage
      .filter(t => !isWildcard(t._2))
      .keySet
  }

  def numValues:Int

  def isWildcard(a: T) :Boolean

  def countChanges(changeCounter:FieldChangeCounter):(Int,Int)

  def nonWildCardValues:Iterable[T]

  def tryMergeWithConsistent[V <: TemporalFieldTrait[T]](y: V): Option[V]

  def mergeWithConsistent[V <: TemporalFieldTrait[T]](y: V): V

  /** *
   * creates a new field lineage sket by appending all values in y to the back of this one
   *
   * @param y
   * @return
   */
  def append[V <: TemporalFieldTrait[T]](y: V): V

  def firstTimestamp: LocalDate

  def lastTimestamp: LocalDate

  def getValueLineage: mutable.TreeMap[LocalDate, T]

  def toIntervalRepresentation: mutable.TreeMap[TimeInterval, T]

  //gets the hash values at the specified time-intervals, substituting missing values with the hash-value of ReservedChangeValues.NOT_EXISTANT_ROW
  def valuesAt(timeToExtract: TimeIntervalSequence): Map[TimeInterval, T]

  def WILDCARDVALUES:Set[T]

}
