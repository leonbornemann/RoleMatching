package de.hpi.dataset_versioning.db_synthesis.baseline.index

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class TupleSetIndex[A](tuples: IndexedSeq[TupleReference[A]],
                       val parentNodesTimestamps:IndexedSeq[LocalDate],
                       val parentNodesKeys:IndexedSeq[A],
                       val wildcardKeyValues:Set[A]) extends IterableTupleIndex[A]{

  val indexableTimestamps = getRelevantTimestamps.diff(parentNodesTimestamps.toSet)
  if(indexableTimestamps.isEmpty){
    tuples.take(1000).foreach(println(_))
    println(parentNodesTimestamps)
    assert(false)
  }

  val indexTimestamp = getBestTimestamp(indexableTimestamps)
  val index = tuples
    .groupBy(getField(_).valueAt(indexTimestamp))
  val iterableKeys = index.keySet.diff(wildcardKeyValues)
  val indexedTimestamps = ArrayBuffer() ++ parentNodesTimestamps ++ Seq(indexTimestamp)

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
      .head._1
  }

  override def tupleGroupIterator:Iterator[TupleGroup[A]] = new TupleGroupIterator()

  class TupleGroupIterator() extends Iterator[TupleGroup[A]] {
    val indexNodeIterator = iterableKeys.iterator

    override def hasNext: Boolean = indexNodeIterator.hasNext

    override def next(): TupleGroup[A] = {
      val key = indexNodeIterator.next()
      val tuples = index(key)
      val wildcards = wildcardKeyValues.toIndexedSeq
        .map(k => index.getOrElse(k,IndexedSeq()))
        .flatten
      TupleGroup(indexedTimestamps,parentNodesKeys ++ Seq(key),tuples,wildcards)
    }
  }

}
