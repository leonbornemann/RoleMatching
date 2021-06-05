package de.hpi.tfm.compatibility.index

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.compatibility.index.BipartiteTupleIndex.totalCallStackHistory
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG

import java.time.LocalDate

class BipartiteTupleIndex[A](tuplesLeftUnfiltered: IndexedSeq[TupleReference[A]],
                          tuplesRightUnfiltered: IndexedSeq[TupleReference[A]],
                          val parentTimestamps:IndexedSeq[LocalDate] = IndexedSeq(),
                          val parentKeyValues:IndexedSeq[A] = IndexedSeq(),
                          ignoreZeroChangeTuples:Boolean = true,
                           monsterLogging:Boolean=false) extends TupleIndexUtility[A] with StrictLogging{
  //logger.debug(s"Input: ${tuplesLeftUnfiltered.size} and ${tuplesRightUnfiltered.size}")
//  private val toAppend: (IndexedSeq[TupleReference[Any]], IndexedSeq[TupleReference[Any]]) =
//    (tuplesLeftUnfiltered.map(_.asInstanceOf[TupleReference[Any]]), tuplesRightUnfiltered.map(_.asInstanceOf[TupleReference[Any]]))
//  if(!totalCallStackHistory.isEmpty && totalCallStackHistory.last==toAppend){
//    println("Whaaat?")
//  }
//  val isWeird = !totalCallStackHistory.isEmpty && totalCallStackHistory.last==toAppend
//  totalCallStackHistory.append(toAppend)


  def numLeafNodes: Int = {
    leftGroups.keySet.union(rightGroups.keySet)
      .filter(!wildcardValues.contains(_))
      .size
  }

  //assert(tuplesLeftUnfiltered.toSet.intersect(tuplesRightUnfiltered.toSet).isEmpty)
  val tuplesLeft = getFilteredTuples(tuplesLeftUnfiltered)
  val tuplesRight = getFilteredTuples(tuplesRightUnfiltered)
  val unusedTimestamps = getRelevantTimestamps(tuplesLeft).union(getRelevantTimestamps(tuplesRight)).diff(parentTimestamps.toSet)
  var indexFailed = false
  var compressionRation = 0.0

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
    else {
      val priorCombinations = tuplesLeft.size * tuplesRight.size
      val bestTimestampCandidates = timestampsToConsider.map(t => {
        val leftGroups = tuplesLeft.groupBy(_.getDataTuple.head.valueAt(t))
        val rightGroups = tuplesRight.groupBy(_.getDataTuple.head.valueAt(t))
        val wildcards = tuplesLeft.head.table.wildcardValues.toSet
        val nonWildCardCombinations = leftGroups
          .withFilter(t => !wildcards.contains(t._1))
          .map{case (k,tuples) => tuples.size*rightGroups.getOrElse(k,IndexedSeq()).size}
          .sum
        //wildcards left:
        val wildcardCountLeft = wildcards.map(wc => leftGroups.getOrElse(wc,IndexedSeq()).size).sum
        val wildcardCountRight = wildcards.map(wc => rightGroups.getOrElse(wc,IndexedSeq()).size).sum
        val wildcardsLeftToRestOfRight = wildcardCountLeft * (tuplesRight.size - wildcardCountRight)
        val wildcardsRightToRestOfLeft = wildcardCountRight * (tuplesLeft.size - wildcardCountLeft)
        val wildcardToWildcardCount = wildcardCountLeft*wildcardCountRight
        val combinationsAfterSplit = nonWildCardCombinations + wildcardsLeftToRestOfRight + wildcardsRightToRestOfLeft + wildcardToWildcardCount
        if(combinationsAfterSplit>priorCombinations){
          logger.debug("More combinations after split - That is bad - this should never happen and is an error in indexing")
          assert(false)
        }
//        if(isWeird && combinationsAfterSplit==0)
//          println()
        (t,combinationsAfterSplit)
      }).toIndexedSeq
      val bestTimestamp = bestTimestampCandidates
        .sortBy(_._2)
        .head
      val compressionRate = 1.0 - bestTimestamp._2 / priorCombinations.toDouble
      compressionRation = 1.0 - bestTimestamp._2 / priorCombinations.toDouble
      if(bestTimestamp._2>=priorCombinations || compressionRate < 0.1) {
        None
      } else {
//        logger.debug(s"Constructed new Index - Compression Rate: $compressionRate")
//        if(compressionRate<0.2) {
//          logger.debug(s"Constructed new Index - Compression Rate: $compressionRate")
//        }
        Some(bestTimestamp)
      }
    }
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
object BipartiteTupleIndex{

  val totalCallStackHistory = scala.collection.mutable.ArrayBuffer[(IndexedSeq[TupleReference[Any]],IndexedSeq[TupleReference[Any]])]()
}
