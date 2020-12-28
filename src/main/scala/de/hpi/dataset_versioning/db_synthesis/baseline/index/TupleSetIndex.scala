package de.hpi.dataset_versioning.db_synthesis.baseline.index

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class TupleSetIndex[A](tuples: IndexedSeq[TupleReference[A]],
                       val parentNodesTimestamps:IndexedSeq[LocalDate],
                       val parentNodesKeys:IndexedSeq[A],
                       val wildcardKeyValues:Set[A]) extends IterableTupleIndex[A]{

  val indexableTimestamps = getRelevantTimestamps.diff(parentNodesTimestamps.toSet)

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

  def getField(tupleReference: TupleReference[A]) = tupleReference.table
    .getDataTuple(tupleReference.rowIndex).head

  private def getRelevantTimestamps = {
    tuples
      .map(r => {
        val a = getField(r)
        a.allTimestamps.toSet
      }).flatten
      .toSet
  }

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
}
object TupleSetIndex{

  def tryBuildIndex[A](tuples: IndexedSeq[TupleReference[A]],
    parentNodesTimestamps:IndexedSeq[LocalDate],
    parentNodesKeys:IndexedSeq[A],
    wildcardKeyValues:Set[A]) = {

  }
}
