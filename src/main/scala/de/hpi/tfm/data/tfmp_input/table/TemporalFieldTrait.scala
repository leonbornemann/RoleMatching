package de.hpi.tfm.data.tfmp_input.table

import de.hpi.tfm.data.socrata.change.temporal_tables.time.{TimeInterval, TimeIntervalSequence}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.tfm.fact_merging.config.{GLOBAL_CONFIG, UpdateChangeCounter}
import de.hpi.tfm.fact_merging.metrics.{EntropyComputer, MutualInformationComputer, UltimateChangeScoreComputer}
import de.hpi.tfm.io.IOService

import java.time.LocalDate
import scala.collection.mutable

trait TemporalFieldTrait[T] {

  def newScore(other: TemporalFieldTrait[T]) = {
    new UltimateChangeScoreComputer(this, other).score()
  }


  //private var entropy:Option[Double] = None

  def getEntropy(): Double = {
    new EntropyComputer(this).entropy
    //    if(entropy.isDefined)
    //      entropy.get
    //    else {
    //      entropy = Some(new EntropyComputer(this).entropy)
    //      entropy.get
    //    }
  }

  def mutualInformation(other: TemporalFieldTrait[T]): Double = new MutualInformationComputer(this, other).mutualInfo()

  def valueTransitions(includeSameValueTransitions: Boolean = false): Set[ValueTransition[T]] = {
    if (!includeSameValueTransitions) {
      val vl = getValueLineage
        .values
        //.filter(!isWildcard(_))
        .toIndexedSeq
      (1 until vl.size).map(i => ValueTransition[T](vl(i - 1), vl(i))).toSet
    } else {
      val vl = getValueLineage.toIndexedSeq
      val transitions = (1 until vl.size)
        .flatMap(i => {
          val prev = vl(i - 1)
          val cur = vl(i)
          //handle prev to prev
          val res = scala.collection.mutable.ArrayBuffer[ValueTransition[T]]()
          if (cur._1.toEpochDay - prev._1.toEpochDay > 1) {
            //add Prev to Prev
            res += ValueTransition(prev._2, prev._2)
          }
          res += ValueTransition(prev._2, cur._2)
          //handle last:
          if (i == vl.size - 1 && cur._1.isBefore(IOService.STANDARD_TIME_FRAME_END)) {
            res += ValueTransition(cur._2, cur._2)
          }
          res
        })
      transitions.toSet
    }
  }


  private def notWCOrEmpty(prevValue1: Option[T]): Boolean = {
    !prevValue1.isEmpty && !isWildcard(prevValue1.get)
  }

  private def getNonWCInterleavedOverlapEvidenceMultiSet(other: TemporalFieldTrait[T]): Option[mutable.HashMap[ValueTransition[T], Int]] = {
    val res = this.tryMergeWithConsistent(other)
    if (!res.isDefined) {
      return None
    }
    val vl1 = this.getValueLineage
    val vl2 = other.getValueLineage
    val vl1Iterator = scala.collection.mutable.Queue() ++ vl1
    val vl2Iterator = scala.collection.mutable.Queue() ++ vl2
    val evidenceSet = mutable.HashMap[ValueTransition[T], Int]()
    var curValue1 = vl1Iterator.dequeue()._2
    var curValue2 = vl2Iterator.dequeue()._2
    var prevValue1: Option[T] = None
    var prevValue2: Option[T] = None
    while ((!vl1Iterator.isEmpty || !vl2Iterator.isEmpty)) {
      assert(isWildcard(curValue1) || isWildcard(curValue2) || curValue1 == curValue2)
      if (vl1Iterator.isEmpty) {
        prevValue2 = Some(curValue2)
        curValue2 = vl2Iterator.dequeue()._2
      } else if (vl2Iterator.isEmpty) {
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
            assert(prevValue1.get == prevValue2.get)
            assert(curValue1 == curValue2)
            val toAdd = ValueTransition(prevValue1.get, curValue1)
            val oldValue = evidenceSet.getOrElse(toAdd, 0)
            evidenceSet(toAdd) = oldValue + 1
          }
        } else if (ts1.isBefore(ts2)) {
          prevValue1 = Some(curValue1)
          curValue1 = vl1Iterator.dequeue()._2
        } else {
          assert(ts2.isBefore(ts1))
          prevValue2 = Some(curValue2)
          curValue2 = vl2Iterator.dequeue()._2
        }
      }

    }
    Some(evidenceSet)
  }

  private def getWCInterleavedOverlapEvidenceMultiSet(other: TemporalFieldTrait[T]): Option[mutable.HashMap[ValueTransition[T], Int]] = {
    val res = this.tryMergeWithConsistent(other)
    if (!res.isDefined) {
      return None
    }
    val vl1 = this.getValueLineage
    val vl2 = other.getValueLineage
    val vl1Iterator = scala.collection.mutable.Queue() ++ vl1
    val vl2Iterator = scala.collection.mutable.Queue() ++ vl2
    val evidenceSet = mutable.HashMap[ValueTransition[T], Int]()
    var curValue1 = vl1Iterator.dequeue()._2
    var curValue2 = vl2Iterator.dequeue()._2
    var prevNonWCValue1: Option[T] = None
    var prevNonWCValue2: Option[T] = None
    while ((!vl1Iterator.isEmpty || !vl2Iterator.isEmpty)) {
      assert(isWildcard(curValue1) || isWildcard(curValue2) || curValue1 == curValue2)
      if (vl1Iterator.isEmpty) {
        if (!isWildcard(curValue2))
          prevNonWCValue2 = Some(curValue2)
        curValue2 = vl2Iterator.dequeue()._2
      } else if (vl2Iterator.isEmpty) {
        if (!isWildcard(curValue1))
          prevNonWCValue1 = Some(curValue1)
        curValue1 = vl1Iterator.dequeue()._2
      } else {
        val ts1 = vl1Iterator.head._1
        val ts2 = vl2Iterator.head._1
        if (ts1 == ts2) {
          if (!isWildcard(curValue1))
            prevNonWCValue1 = Some(curValue1)
          curValue1 = vl1Iterator.dequeue()._2
          if (!isWildcard(curValue2))
            prevNonWCValue2 = Some(curValue2)
          curValue2 = vl2Iterator.dequeue()._2
          if (!isWildcard(curValue1) && !isWildcard(curValue2) &&
            prevNonWCValue1.isDefined && prevNonWCValue2.isDefined &&
            prevNonWCValue1.get == prevNonWCValue2.get &&
            curValue1 != prevNonWCValue1.get
          ) {
            val toAdd = ValueTransition(prevNonWCValue1.get, curValue1)
            val oldValue = evidenceSet.getOrElse(toAdd, 0)
            evidenceSet(toAdd) = oldValue + 1
          }
        } else if (ts1.isBefore(ts2)) {
          if (!isWildcard(curValue1))
            prevNonWCValue1 = Some(curValue1)
          curValue1 = vl1Iterator.dequeue()._2
        } else {
          assert(ts2.isBefore(ts1))
          if (!isWildcard(curValue2))
            prevNonWCValue2 = Some(curValue2)
          curValue2 = vl2Iterator.dequeue()._2
        }
      }

    }
    Some(evidenceSet)
  }

  //=GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS
  def getOverlapEvidenceMultiSet(other: TemporalFieldTrait[T]): collection.Map[ValueTransition[T], Int] = getOverlapEvidenceMultiSet(other, GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS)

  def getOverlapEvidenceCount(other: TemporalFieldTrait[T]): Int = getOverlapEvidenceCount(other, GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS)

  def getOverlapEvidenceMultiSet(other: TemporalFieldTrait[T], allowInterleavedWildcards: Boolean) = {
    if (!allowInterleavedWildcards) {
      val res = getNonWCInterleavedOverlapEvidenceMultiSet(other)
      res.get
    }
    else {
      val res = getWCInterleavedOverlapEvidenceMultiSet(other)
      res.get
    }
  }

  def getOverlapEvidenceCount(other: TemporalFieldTrait[T], allowInterleavedWildcards: Boolean) = {
    if (!allowInterleavedWildcards) {
      val multiSet = getNonWCInterleavedOverlapEvidenceMultiSet(other)
      if (multiSet.isDefined) multiSet.get.values.sum else -1
    } else {
      val multiSet = getWCInterleavedOverlapEvidenceMultiSet(other)
      if (multiSet.isDefined) multiSet.get.values.sum else -1
    }
  }

  def valueAt(ts: LocalDate): T

  def allTimestamps: Iterable[LocalDate] = getValueLineage.keySet

  def allNonWildcardTimestamps: Iterable[LocalDate] = {
    getValueLineage
      .filter(t => !isWildcard(t._2))
      .keySet
  }

  def numValues: Int

  def isWildcard(a: T): Boolean

  def countChanges(changeCounter: UpdateChangeCounter): (Int, Int)

  def nonWildCardValues: Iterable[T]

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

  def WILDCARDVALUES: Set[T]

}
