package de.hpi.tfm.compatibility.index

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG

import java.time.LocalDate

class BipartiteTupleIndex[A](tuplesLeftUnfiltered: IndexedSeq[TupleReference[A]],
                          tuplesRightUnfiltered: IndexedSeq[TupleReference[A]],
                          val parentTimestamps:IndexedSeq[LocalDate] = IndexedSeq(),
                          val parentKeyValues:IndexedSeq[A] = IndexedSeq(),
                          ignoreZeroChangeTuples:Boolean = true) extends TupleIndexUtility[A] with StrictLogging{
  def numLeafNodes: Int = {
    leftGroups.keySet.union(rightGroups.keySet)
      .filter(!wildcardValues.contains(_))
      .size
  }


  assert(tuplesLeftUnfiltered.toSet.intersect(tuplesRightUnfiltered.toSet).isEmpty)

  val tuplesLeft = getFilteredTuples(tuplesLeftUnfiltered)
  val tuplesRight = getFilteredTuples(tuplesRightUnfiltered)
  val unusedTimestamps = getRelevantTimestamps(tuplesLeft).union(getRelevantTimestamps(tuplesRight))
  var indexFailed = false

  def getPriorEntropy(tuplesLeft: IndexedSeq[TupleReference[A]], tuplesRight: IndexedSeq[TupleReference[A]]) = {
    val pLeft = tuplesLeft.size / (tuplesLeft.size +tuplesRight.size).toDouble
    val pRight = tuplesRight.size / (tuplesLeft.size + tuplesRight.size).toDouble
    -(pLeft*log2(pLeft) + pRight*log2(pRight))
  }

  def log2(a: Double) = math.log(a) / math.log(2)

  def getBestSplitTimestamp(tuplesLeft:IndexedSeq[TupleReference[A]],
                            tuplesRight:IndexedSeq[TupleReference[A]],
                            timestampsToConsider:Set[LocalDate]) = {
    if(timestampsToConsider.size==0)
      None
    val priorCombinations = tuplesLeft.size * tuplesRight.size
    val bestTimestamp = timestampsToConsider.map(t => {
      val leftGroups = tuplesLeft.groupBy(_.getDataTuple.head.valueAt(t))
      val rightGroups = tuplesRight.groupBy(_.getDataTuple.head.valueAt(t))
      val wildcards = tuplesLeft.head.table.wildcardValues.toSet
      val nonWildCardCombinations = leftGroups
        .withFilter(t => !wildcards.contains(t._1))
        .map{case (k,tuples) => tuples.size*rightGroups.getOrElse(k,IndexedSeq()).size}
        .sum
      //wildcards left:
      val wildcardsLeftSum = wildcards.map(wc => leftGroups.getOrElse(wc,IndexedSeq()).size * tuplesRight.size).sum
      val wildcardsRightSum = wildcards.map(wc => rightGroups.getOrElse(wc,IndexedSeq()).size * tuplesLeft.size).sum
      val combinationsAfterSplit = nonWildCardCombinations + wildcardsLeftSum + wildcardsRightSum
      (t,combinationsAfterSplit)
    }).toIndexedSeq
      .sortBy(_._2)
      .head
    if(bestTimestamp._2>=priorCombinations)
      None
    else
      Some(bestTimestamp)
  }

  //just a single layer for now:
  var curBestSplitTimestamp = getBestSplitTimestamp(tuplesLeft,tuplesRight,unusedTimestamps)

  //init index:
  var splitT:LocalDate = null
  var wildcardValues:Set[A] = null
  var leftGroups:Map[A, IndexedSeq[TupleReference[A]]] = null
  var rightGroups:Map[A, IndexedSeq[TupleReference[A]]] = null
  var wildcardsLeft: IndexedSeq[TupleReference[A]] = null
  var wildcardsRight: IndexedSeq[TupleReference[A]] = null
  if(curBestSplitTimestamp.isEmpty)
    indexFailed = true
  else {
    indexFailed=false
    splitT = curBestSplitTimestamp.get._1
    wildcardValues = tuplesLeft.head.table.wildcardValues.toSet
    leftGroups = tuplesLeft.groupBy(_.getDataTuple.head.valueAt(splitT))
    rightGroups = tuplesRight.groupBy(_.getDataTuple.head.valueAt(splitT))
    wildcardsLeft = wildcardValues.flatMap(wc => leftGroups.getOrElse(wc,IndexedSeq())).toIndexedSeq
    wildcardsRight = wildcardValues.flatMap(wc => rightGroups.getOrElse(wc,IndexedSeq())).toIndexedSeq
  }
  val chosenTimestamps = scala.collection.mutable.ArrayBuffer() ++ parentTimestamps ++ Seq(splitT)

  private def getFilteredTuples(tuples:IndexedSeq[TupleReference[A]]) = {
    tuples.filter(tr => !ignoreZeroChangeTuples || tr.getDataTuple.head.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)._1>0)
  }

  def getBipartiteTupleGroupIterator():Iterator[BipartiteTupleGroup[A]] = {
    if(indexFailed)
      throw new AssertionError("No groups to iterate over")
    else {
      BipartiteTupleGroupIterator()
    }
  }

  case class BipartiteTupleGroupIterator() extends Iterator[BipartiteTupleGroup[A]]{
    val keys = leftGroups.keySet.union(rightGroups.keySet)
    val keysWithOutWCIt = keys
      .filter(!wildcardValues.contains(_))
      .iterator

    override def hasNext: Boolean = keysWithOutWCIt.hasNext

    override def next(): BipartiteTupleGroup[A] = {
      val k = keysWithOutWCIt.next()
      val left = leftGroups.getOrElse(k,IndexedSeq())
      val right = rightGroups.getOrElse(k,IndexedSeq())
      BipartiteTupleGroup[A](chosenTimestamps,parentKeyValues ++ IndexedSeq(k),wildcardsLeft,wildcardsRight,left,right)
    }
  }

}
