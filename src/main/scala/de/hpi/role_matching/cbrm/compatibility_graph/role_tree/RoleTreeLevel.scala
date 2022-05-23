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
      val timestampOption = getBestTimestamp(indexableTimestamps)
      if(timestampOption.isDefined) {
        indexTimestamp = timestampOption.get._1
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
    val priorCombinations = tuples.size.toLong * tuples.size.toLong
    val bestTimestamp = timestampSample.map(ts => {
      val groups = tupleSample.groupBy(getField(_).valueAt(ts))
      val wildcards = tupleSample.head.roles.wildcardValues.toSet
      val wildcardBucketSize = wildcards.map(wc => groups.getOrElse(wc,IndexedSeq()).size).sum
      val sizeOfNonWildcards = tupleSample.size - wildcardBucketSize
      val allIndividualSizes = groups
        .withFilter{case (k,_) => !wildcards.contains(k)}
        .map{case (_,v) => v.size.toLong*v.size.toLong}
        .sum
      val totalAfterSplit = wildcardBucketSize.toLong * (wildcardBucketSize.toLong + sizeOfNonWildcards.toLong) + allIndividualSizes
      if(!(totalAfterSplit <=priorCombinations))
        println()
      assert(totalAfterSplit <=priorCombinations)
      (ts,totalAfterSplit)
    }).sortBy(_._2)
      .head
    val compressionRate = 1.0 - bestTimestamp._2 / priorCombinations.toDouble
    if(compressionRate < 0.1 )
      None
    else
      Some(bestTimestamp)
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
