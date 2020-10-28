package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.index.LayeredTupleIndex
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

import scala.collection.mutable

class PairwiseTupleMapper[A](tableA: TemporalDatabaseTableTrait[A], tableB: TemporalDatabaseTableTrait[A], index: LayeredTupleIndex[A], mapping: collection.Map[Set[AttributeLineage], Set[AttributeLineage]]) {

  val aColsByID = tableA.dataColumns.map(c => (c.attrID,c)).toMap
  val bColsByID = tableB.dataColumns.map(c => (c.attrID,c)).toMap
  val insertTimeA = tableA.insertTime
  val insertTimeB = tableB.insertTime
  val mergedInsertTime = Seq(tableA.insertTime,tableB.insertTime).min
  val isSurrogateBased = tableA.isSurrogateBased
  if(tableA.isSurrogateBased) assert(tableB.isSurrogateBased)

  def sizesAreTooBig(tupleCount1:Int, tupleCount2:Int): Boolean = {
    tupleCount1*tupleCount2>100000
  }

  def mapGreedy() = {
    val finalMatching:TupleSetMatching[A] = new TupleSetMatching[A](tableA,tableB)
    index.tupleGroupIterator.foreach(g => {
      val byTable = g.groupMap(_._1)(_._2)
      if(!byTable.contains(tableA)){
        val tableBTuples = byTable(tableB)
        finalMatching ++= new TupleSetMatching(tableA,tableB,mutable.HashSet(),mutable.HashSet() ++ tableBTuples)
      } else if(!byTable.contains(tableB)){
        val tableATuples = byTable(tableA)
        finalMatching ++= new TupleSetMatching(tableA,tableB,mutable.HashSet() ++ tableATuples,mutable.HashSet())
      } else{
        val tableATuples = byTable(tableA)
        val tableBTuples = byTable(tableB)
        //we can do an easy check here: if pairwise-checking is too expensive, we further continue indexing
        if(sizesAreTooBig(tableATuples.size,tableBTuples.size)){
          ??? //build more indices to further split the group
        } else{
          val matchingForGroup = getBestTupleMatching(tableATuples.toIndexedSeq,tableBTuples.toIndexedSeq)
          finalMatching ++= matchingForGroup
        }
      }
    })
    finalMatching
  }

  def mergeTupleSketches(mappedFieldLineages:collection.Map[TemporalFieldTrait[A], TemporalFieldTrait[A]]) = {
    mappedFieldLineages.map{case (a,b) => a.tryMergeWithConsistent(b)}.toSeq
  }

  def buildTuples(tupA: Int, tupB: Int) = {
    mapping.map{case (a,b) => {
      val lineagesA = a.toIndexedSeq
        .map(al => aColsByID(al.attrId).fieldLineages(tupA))
        .sortBy(_.lastTimestamp.toEpochDay)
      val lineagesB = b.toIndexedSeq
        .map(al => bColsByID(al.attrId).fieldLineages(tupB))
        .sortBy(_.lastTimestamp.toEpochDay)
      val aConcatenated = if(lineagesA.size==1) lineagesA.head else lineagesA.reduce((x,y) => x.mergeWithConsistent(y))
      val bConcatenated = if(lineagesB.size==1) lineagesB.head else lineagesB.reduce((x,y) => x.mergeWithConsistent(y))
      (aConcatenated,bConcatenated)
    }}
  }

  def countChanges(tuple: collection.Seq[TemporalFieldTrait[A]],insertTime:LocalDate) = {
    if(isSurrogateBased){
      GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countFieldChanges(tuple)
    } else {
      ???
      //tuple.map(_.countChanges(insertTime, GLOBAL_CONFIG.CHANGE_COUNT_METHOD)).sum
    }
  }

  def getBestTupleMatching(tuplesA: collection.IndexedSeq[Int], tuplesB: collection.IndexedSeq[Int]) = {
    //we do simple pairwise matching here until we find out its a problem
    val tuplesBRemaining = mutable.HashSet() ++ tuplesB
    val unmatchedTuplesA = mutable.HashSet[Int]()
    var tupleMatching = mutable.ArrayBuffer[TupleMatching]()
    for(tupA<-tuplesA){
      var bestMatchScore = 0
      var curBestTupleB = -1
      val originalTupleA = tableA.getDataTuple(tupA)
      for(tupB <-tuplesBRemaining){
        //apply mapping
        val originalTupleB = tableB.getDataTuple(tupB)
        val mappedFieldLineages = buildTuples(tupA,tupB) // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
        val mergedTupleOptions = mergeTupleSketches(mappedFieldLineages)
        if(mergedTupleOptions.exists(_.isEmpty)){
          //illegalMatch - we do nothing
        } else{
          val mergedTuple = mergedTupleOptions.map(_.get)
          val sizeAfterMerge = countChanges(mergedTuple,mergedInsertTime)//
          val sizeBeforeMergeA = countChanges(originalTupleA,insertTimeA)
          val sizeBeforeMergeB = countChanges(originalTupleB,insertTimeB)
          val curScore = sizeBeforeMergeA+sizeBeforeMergeB-sizeAfterMerge
          if(curScore>bestMatchScore){
            bestMatchScore = curScore
            curBestTupleB = tupB
          }
        }
      }
      if(curBestTupleB!= -1){
        tupleMatching +=TupleMatching(tupA,curBestTupleB,bestMatchScore)
        tuplesBRemaining.remove(curBestTupleB)
      } else{
        unmatchedTuplesA.add(tupA)
      }
    }
    new TupleSetMatching(tableA,tableB,unmatchedTuplesA,tuplesBRemaining,tupleMatching)
  }
}
object PairwiseTupleMapper extends StrictLogging{
  logger.debug("Current implementation does not yet reduce the size of the tuple groups - this still needs to be implemented")
}
