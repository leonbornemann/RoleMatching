package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree
import de.hpi.role_matching.cbrm.data.RoleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class RoleTreeLevel(private var tuples: IndexedSeq[RoleReference],
                       val parentNodesTimestamps:IndexedSeq[LocalDate],
                       val parentNodesKeys:IndexedSeq[Any],
                       val wildcardKeyValues:Set[Any],
                       val ignoreTuplesWithNoChanges:Boolean) extends IterableRoleIndex{

  if(ignoreTuplesWithNoChanges)
    tuples = tuples.filter(a => a.getRole.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)._1 > 0)

  val indexableTimestamps = getRelevantTimestamps(tuples).diff(parentNodesTimestamps.toSet)

  var indexTimestamp:LocalDate = null
  var index:Map[Any, IndexedSeq[RoleReference]] = null
  var iterableKeys:Set[Any] = null
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
    val tupleSample:IndexedSeq[RoleReference] = getRandomSample(tuples,GLOBAL_CONFIG.INDEXING_CONFIG.samplingRateRoles)
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

  override def tupleGroupIterator(skipWildCardBuckets: Boolean):Iterator[RoleTreePartition] = new TupleGroupIterator(skipWildCardBuckets)

  class TupleGroupIterator(skipWildCardBuckets: Boolean) extends Iterator[RoleTreePartition] {
    assert(skipWildCardBuckets)
    val indexNodeIterator = iterableKeys.iterator

    override def hasNext: Boolean = indexNodeIterator.hasNext

    override def next(): RoleTreePartition = {
      val key = indexNodeIterator.next()
      val tuples = index(key)
      val wildcards = getWildcardBucket
      role_tree.RoleTreePartition(indexedTimestamps,parentNodesKeys ++ Seq(key),tuples,wildcards)
    }
  }

  override def wildcardBuckets: IndexedSeq[RoleTreePartition] = wildcardKeyValues
    .map(k => role_tree.RoleTreePartition(indexedTimestamps,parentNodesKeys ++ Seq(k),IndexedSeq(),index.getOrElse(k,IndexedSeq())))
    .toIndexedSeq

  override def getParentKeyValues: IndexedSeq[Any] = parentNodesKeys
}
