package de.hpi.dataset_versioning.db_synthesis.baseline.index

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

import java.lang.AssertionError
import java.time.LocalDate

class BipartiteTupleIndex(tablesLeft: IndexedSeq[TemporalDatabaseTableTrait[Int]],
                          tablesRight: IndexedSeq[TemporalDatabaseTableTrait[Int]],
                          ignoreZeroChangeTuples:Boolean = true) extends TupleIndexUtility[Int] with StrictLogging{
  def getWildcardsLeft() = {

  }


  assert(tablesLeft.toSet.intersect(tablesRight.toSet).isEmpty)

  val tuplesLeft = getTuples(tablesLeft)
  val tuplesRight = getTuples(tablesRight)
  val unusedTimestamps = getRelevantTimestamps(tuplesLeft).union(getRelevantTimestamps(tuplesRight))
  var indexFailed = true

  def getPriorEntropy(tuplesLeft: IndexedSeq[TupleReference[Int]], tuplesRight: IndexedSeq[TupleReference[Int]]) = {
    val pLeft = tuplesLeft.size / (tuplesLeft.size +tuplesRight.size).toDouble
    val pRight = tuplesRight.size / (tuplesLeft.size + tuplesRight.size).toDouble
    -(pLeft*log2(pLeft) + pRight*log2(pRight))
  }

  def log2(a: Double) = math.log(a) / math.log(2)

  def getBestSplitTimestamp(tuplesLeft:IndexedSeq[TupleReference[Int]],
                            tuplesRight:IndexedSeq[TupleReference[Int]],
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
      if(combinationsAfterSplit>priorCombinations){
        logger.debug(s"This is definisoundtely a bug: $priorCombinations $combinationsAfterSplit")
      }
      assert(combinationsAfterSplit<=priorCombinations)
      (t,combinationsAfterSplit)
    }).toIndexedSeq
      .sortBy(_._2)
      .head
    Some(bestTimestamp)
  }

  //just a single layer for now:
  var curBestSplitTimestamp = getBestSplitTimestamp(tuplesLeft,tuplesRight,unusedTimestamps)

  //init index:
  var splitT:LocalDate = null
  var wildcardValues:Set[Int] = null
  var leftGroups:Map[Int, IndexedSeq[TupleReference[Int]]] = null
  var rightGroups:Map[Int, IndexedSeq[TupleReference[Int]]] = null
  var wildcardsLeft: IndexedSeq[TupleReference[Int]] = null
  var wildcardsRight: IndexedSeq[TupleReference[Int]] = null
  if(curBestSplitTimestamp.isDefined)
    indexFailed = false
  else {
    splitT = curBestSplitTimestamp.get._1
    wildcardValues = tuplesLeft.head.table.wildcardValues.toSet
    leftGroups = tuplesLeft.groupBy(_.getDataTuple.head.valueAt(splitT))
    rightGroups = tuplesRight.groupBy(_.getDataTuple.head.valueAt(splitT))
    wildcardsLeft = wildcardValues.flatMap(wc => leftGroups(wc)).toIndexedSeq
    wildcardsRight = wildcardValues.flatMap(wc => rightGroups(wc)).toIndexedSeq
  }

  private def getTuples(tables:IndexedSeq[TemporalDatabaseTableTrait[Int]]) = {
    tables.flatMap(t => (0 until t.nrows)
      .withFilter(i => !ignoreZeroChangeTuples || t.getDataTuple(i).head.countChanges(GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD)._1>0)
      .map(i => new TupleReference[Int](t, i))
    )
  }

  def getBipartiteTupleGroupIterator():Iterator[BipartiteTupleGroup[Int]] = {
    if(indexFailed)
      throw new AssertionError("No groups to iterate over")
    else {
      BipartiteTupleGroupIterator()
    }
  }


  case class BipartiteTupleGroupIterator() extends Iterator[BipartiteTupleGroup[Int]]{
    val keys = leftGroups.keySet.union(rightGroups.keySet)
    val chosenTimestamps = scala.collection.mutable.ArrayBuffer(splitT)
    val keysWithOutWCIt = keys
      .filter(!wildcardValues.contains(_))
      .iterator

    override def hasNext: Boolean = keysWithOutWCIt.hasNext

    override def next(): BipartiteTupleGroup[Int] = {
      val k = keysWithOutWCIt.next()
      val left = leftGroups.getOrElse(k,IndexedSeq())
      val right = rightGroups.getOrElse(k,IndexedSeq())
      BipartiteTupleGroup[Int](chosenTimestamps,IndexedSeq(k),wildcardsLeft,wildcardsRight,left,right)
    }
  }

}
