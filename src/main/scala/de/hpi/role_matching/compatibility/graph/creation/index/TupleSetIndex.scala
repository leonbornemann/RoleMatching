package de.hpi.role_matching.compatibility.graph.creation.index

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.compatibility.graph.creation.TupleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class TupleSetIndex[A](private var tuples: IndexedSeq[TupleReference[A]],
                       val parentNodesTimestamps:IndexedSeq[LocalDate],
                       val parentNodesKeys:IndexedSeq[A],
                       val wildcardKeyValues:Set[A],
                       val ignoreTuplesWithNoChanges:Boolean) extends IterableTupleIndex[A]{

  if(ignoreTuplesWithNoChanges)
    tuples = tuples.filter(a => a.getDataTuple.head.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)._1 > 0)

  val indexableTimestamps = getRelevantTimestamps(tuples).diff(parentNodesTimestamps.toSet)

  var indexTimestamp:LocalDate = null
  var index:Map[A, IndexedSeq[TupleReference[A]]] = null
  var iterableKeys:Set[A] = null
  var indexedTimestamps:ArrayBuffer[LocalDate] = null

  if(!indexableTimestamps.isEmpty){
    if(tuples.size>1){
      val (bestIndexTimestamp,numValues) = getBestTimestamp(indexableTimestamps)
      if(numValues>=1) {
        indexTimestamp = bestIndexTimestamp
        index = tuples
          .groupBy(getField(_).valueAt(indexTimestamp))
        iterableKeys = index.keySet.diff(wildcardKeyValues)
        indexedTimestamps = ArrayBuffer() ++ parentNodesTimestamps ++ Seq(indexTimestamp)
      }
    }
  }

  def indexBuildWasSuccessfull = indexTimestamp!=null

  def getBestTimestamp(relevantTimestamps: Set[LocalDate]) = {
    val tupleSample:IndexedSeq[TupleReference[A]] = getRandomSample(tuples,GLOBAL_CONFIG.INDEXING_CONFIG.samplingRateRoles)
    val timestampSample:IndexedSeq[LocalDate] = getRandomSample(relevantTimestamps.toIndexedSeq,GLOBAL_CONFIG.INDEXING_CONFIG.samplingRateTimestamps)
    timestampSample.map(ts => {
      val values = tupleSample.map(getField(_).valueAt(ts)).toSet
      (ts,values.size)
    }).sortBy(-_._2)
      .head
  }

  def getWildcardBucket = wildcardKeyValues.toIndexedSeq
    .map(k => index.getOrElse(k,IndexedSeq()))
    .flatten

  override def tupleGroupIterator(skipWildCardBuckets: Boolean):Iterator[TupleGroup[A]] = new TupleGroupIterator(skipWildCardBuckets)

  class TupleGroupIterator(skipWildCardBuckets: Boolean) extends Iterator[TupleGroup[A]] {
    assert(skipWildCardBuckets)
    val indexNodeIterator = iterableKeys.iterator

    override def hasNext: Boolean = indexNodeIterator.hasNext

    override def next(): TupleGroup[A] = {
      val key = indexNodeIterator.next()
      val tuples = index(key)
      val wildcards = getWildcardBucket
      TupleGroup(indexedTimestamps,parentNodesKeys ++ Seq(key),tuples,wildcards)
    }
  }

  override def wildcardBuckets: IndexedSeq[TupleGroup[A]] = wildcardKeyValues
    .map(k => TupleGroup(indexedTimestamps,parentNodesKeys ++ Seq(k),IndexedSeq(),index.getOrElse(k,IndexedSeq())))
    .toIndexedSeq

  override def getParentKeyValues: IndexedSeq[A] = parentNodesKeys
}
object TupleSetIndex{

  def tryBuildIndex[A](tuples: IndexedSeq[TupleReference[A]],
    parentNodesTimestamps:IndexedSeq[LocalDate],
    parentNodesKeys:IndexedSeq[A],
    wildcardKeyValues:Set[A]) = {

  }
}
