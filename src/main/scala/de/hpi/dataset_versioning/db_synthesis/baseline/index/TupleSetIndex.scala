package de.hpi.dataset_versioning.db_synthesis.baseline.index

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

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
    val (bestIndexTimestamp,numValues) = getBestTimestamp(indexableTimestamps)
    if(numValues>1) {
      indexTimestamp = bestIndexTimestamp
      index = tuples
        .groupBy(getField(_).valueAt(indexTimestamp))
      iterableKeys = index.keySet.diff(wildcardKeyValues)
      indexedTimestamps = ArrayBuffer() ++ parentNodesTimestamps ++ Seq(indexTimestamp)
    } else{
      //no point in indexing
    }
  }

  def indexBuildWasSuccessfull = indexTimestamp!=null

  def getBestTimestamp(relevantTimestamps: Set[LocalDate]) = {
    relevantTimestamps.map(ts => {
      val values = tuples.map(getField(_).valueAt(ts)).toSet
      (ts,values.size)
    }).toIndexedSeq
      .sortBy(-_._2)
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
}
object TupleSetIndex{

  def tryBuildIndex[A](tuples: IndexedSeq[TupleReference[A]],
    parentNodesTimestamps:IndexedSeq[LocalDate],
    parentNodesKeys:IndexedSeq[A],
    wildcardKeyValues:Set[A]) = {

  }
}
