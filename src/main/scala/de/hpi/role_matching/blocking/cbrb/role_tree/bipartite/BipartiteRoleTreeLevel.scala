package de.hpi.role_matching.blocking.cbrb.role_tree.bipartite

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.blocking.cbrb.role_tree.{RoleTreeUtility, bipartite}
import de.hpi.role_matching.data.RoleReference
import de.hpi.util.GLOBAL_CONFIG

import java.time.LocalDate

class BipartiteRoleTreeLevel(tuplesLeftUnfiltered: IndexedSeq[RoleReference],
                                tuplesRightUnfiltered: IndexedSeq[RoleReference],
                                val parentTimestamps:IndexedSeq[LocalDate] = IndexedSeq(),
                                val parentKeyValues:IndexedSeq[Any] = IndexedSeq(),
                                ignoreZeroChangeTuples:Boolean = true) extends RoleTreeUtility with StrictLogging{


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

  def getPriorEntropy(tuplesLeft: IndexedSeq[RoleReference], tuplesRight: IndexedSeq[RoleReference]) = {
    val pLeft = tuplesLeft.size / (tuplesLeft.size +tuplesRight.size).toDouble
    val pRight = tuplesRight.size / (tuplesLeft.size + tuplesRight.size).toDouble
    -(pLeft*log2(pLeft) + pRight*log2(pRight))
  }

  def log2(a: Double) = math.log(a) / math.log(2)

  def getBestSplitTimestamp(tuplesLeft:IndexedSeq[RoleReference],
                            tuplesRight:IndexedSeq[RoleReference],
                            timestampsToConsider:Set[LocalDate]) = {
    if(timestampsToConsider.size==0)
      None
    else {
      val priorCombinations = tuplesLeft.size * tuplesRight.size
      val bestTimestampCandidates = timestampsToConsider.map(t => {
        val leftGroups = tuplesLeft.groupBy(_.getRole.valueAt(t))
        val rightGroups = tuplesRight.groupBy(_.getRole.valueAt(t))
        val wildcards = tuplesLeft.head.roles.wildcardValues.toSet
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
  val sampleLeft = getRandomSample(tuplesLeft,GLOBAL_CONFIG.INDEXING_CONFIG.samplingRateRoles)
  val sampleRight = getRandomSample(tuplesRight,GLOBAL_CONFIG.INDEXING_CONFIG.samplingRateRoles)
  val sampledTimestamps = getRandomSample(unusedTimestamps.toIndexedSeq,GLOBAL_CONFIG.INDEXING_CONFIG.samplingRateTimestamps)
  var curBestSplitTimestamp = getBestSplitTimestamp(sampleLeft,sampleRight,sampledTimestamps.toSet)

  //init index:
  var splitT:LocalDate = null
  var wildcardValues:Set[Any] = null
  var leftGroups:Map[Any, IndexedSeq[RoleReference]] = null
  var rightGroups:Map[Any, IndexedSeq[RoleReference]] = null
  var wildcardsLeft: IndexedSeq[RoleReference] = null
  var wildcardsRight: IndexedSeq[RoleReference] = null
  if(curBestSplitTimestamp.isEmpty)
    indexFailed = true
  else {
    indexFailed=false
    splitT = curBestSplitTimestamp.get._1
    wildcardValues = tuplesLeft.head.roles.wildcardValues.toSet
    leftGroups = tuplesLeft.groupBy(_.getRole.valueAt(splitT))
    rightGroups = tuplesRight.groupBy(_.getRole.valueAt(splitT))
    wildcardsLeft = wildcardValues.flatMap(wc => leftGroups.getOrElse(wc,IndexedSeq())).toIndexedSeq
    wildcardsRight = wildcardValues.flatMap(wc => rightGroups.getOrElse(wc,IndexedSeq())).toIndexedSeq
  }
  val chosenTimestamps = scala.collection.mutable.ArrayBuffer() ++ parentTimestamps ++ Seq(splitT)

  private def getFilteredTuples(tuples:IndexedSeq[RoleReference]) = {
    tuples.filter(tr => !ignoreZeroChangeTuples || tr.getRole.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)._1>0)
  }

  def getBipartiteTupleGroupIterator():Iterator[BipartiteRolePartition] = {
    if(indexFailed)
      throw new AssertionError("No groups to iterate over")
    else {
      BipartiteTupleGroupIterator()
    }
  }

  case class BipartiteTupleGroupIterator() extends Iterator[BipartiteRolePartition]{
    val keys = leftGroups.keySet.union(rightGroups.keySet)
    val keysWithOutWCIt = keys
      .filter(!wildcardValues.contains(_))
      .iterator

    override def hasNext: Boolean = keysWithOutWCIt.hasNext

    override def next(): BipartiteRolePartition = {
      val k = keysWithOutWCIt.next()
      val left = leftGroups.getOrElse(k,IndexedSeq())
      val right = rightGroups.getOrElse(k,IndexedSeq())
      bipartite.BipartiteRolePartition(chosenTimestamps,parentKeyValues ++ IndexedSeq(k),wildcardsLeft,wildcardsRight,left,right)
    }
  }

}
object BipartiteRoleTreeLevel{

  val totalCallStackHistory = scala.collection.mutable.ArrayBuffer[(IndexedSeq[RoleReference],IndexedSeq[RoleReference])]()
}
