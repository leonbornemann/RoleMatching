package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import java.time.LocalDate
import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.index.{LayeredTupleIndex, TableTupleFindIndex, TupleSetIndex}
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

import scala.collection.mutable

class PairwiseTupleMapper[A](tableA: TemporalDatabaseTableTrait[A], tableB: TemporalDatabaseTableTrait[A], mapping: collection.Map[Set[AttributeLineage], Set[AttributeLineage]]) {

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

  def gaussSum(n: Int) = n*n+1/2

  def squareProductTooBig(n:Int): Boolean = {
    if(gaussSum(n) > 10000) true else false
  }

  def getOptimalMapping() = {
    val tuples = IndexedSeq(tableA,tableB)
      .map(t => (0 until t.nrows).map( r => TupleReference(t,r)))
      .flatten
    val index = new TupleSetIndex[A](tuples,IndexedSeq(),IndexedSeq(),tableA.wildcardValues.toSet)
    val edges = mutable.HashSet[General_1_to_1_TupleMatching[A]]()
    buildGraph(index,edges)
    val graphBasedTupleMapper = new GraphBasedTupleMapper(edges)
    graphBasedTupleMapper.mapGreedy()
  }

  def buildGraph(index: TupleSetIndex[A],edges:mutable.HashSet[General_1_to_1_TupleMatching[A]]):Unit = {
    index.tupleGroupIterator.foreach{case g => {
      val tuplesInNode = (g.tuplesInNode ++ g.wildcardTuples)
      if(squareProductTooBig(tuplesInNode.size)){
        //further index this: new Index
        val newIndexForSubNode = new TupleSetIndex[A](tuplesInNode.toIndexedSeq,index.indexedTimestamps.toIndexedSeq,g.valuesAtTimestamps,index.wildcardKeyValues)
        buildGraph(newIndexForSubNode,edges)
      } else{
        val tuplesInNodeAsIndexedSeq = tuplesInNode.toIndexedSeq
        //we construct a graph as an adjacency list:
        //pairwise matching to find out the edge-weights:
        for( i <- 0 until tuplesInNodeAsIndexedSeq.size){
          for( j <- i+1 until tuplesInNodeAsIndexedSeq.size){
            val ref1 = tuplesInNodeAsIndexedSeq(i)
            val ref2 = tuplesInNodeAsIndexedSeq(j)
            val edge = getTupleMatchOption(ref1, ref2)
            if(edge.isDefined)
              edges.add(edge.get)
          }
        }
      }
    }}
  }

  private def getTupleMatchOption(ref1:TupleReference[A], ref2:TupleReference[A]) = {
    val originalTupleA = ref1.getDataTuple
    val originalTupleB = ref2.getDataTuple
    val mappedFieldLineages = buildTuples(ref1, ref2) // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
    val mergedTupleOptions = mergeTupleSketches(Map(mappedFieldLineages))
    if (mergedTupleOptions.exists(_.isEmpty)) {
      //illegalMatch - we do nothing
      None
    } else {
      val mergedTuple = mergedTupleOptions.map(_.get)
      val sizeAfterMerge = countChanges(mergedTuple, mergedInsertTime) //
      val sizeBeforeMergeA = countChanges(originalTupleA, insertTimeA)
      val sizeBeforeMergeB = countChanges(originalTupleB, insertTimeB)
      val score = sizeBeforeMergeA + sizeBeforeMergeB - sizeAfterMerge
      if (score < 0) {
        //debug
        println(ref1)
        println(ref2)
        println(originalTupleA)
        println(originalTupleB)
        println(mergedTuple)
      }
      assert(score > 0)
      assert(false) //TODO: think about edge order!
      Some(General_1_to_1_TupleMatching(ref1,ref2, score))
    }
  }

  def buildTuples(ref1: TupleReference[A], ref2: TupleReference[A]) = {
    val lineages1 = ref1.getDataTuple
    val lineages2 = ref2.getDataTuple
    assert(lineages1.size==1 && lineages2.size==2)
    (lineages1.head,lineages2.head)
  }

  def mapGreedy() = {
    val index = new TableTupleFindIndex[A](tableA,tableB)
    val oneToManyMatchings = mutable.HashMap[Int,mutable.ArrayBuffer[Int]]()
    val finalMatching:TupleSetMatching[A] = new TupleSetMatching[A](tableA,tableB)
    index.tupleGroupIterator.foreach{case g => {
      val tuplesInNode = g.tuplesInNode
      val byTable = tuplesInNode.groupMap(_.table)(_.rowIndex)
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
          //recursively expand the index
          println(g.chosenTimestamps)
          println(tableA.toString)
          println(tableB.toString)
          println((tableA.getUnionedOriginalTables))
          println((tableB.getUnionedOriginalTables))
          ??? //build more indices to further split the group
        } else{
          val matchingForGroup = getBestTupleMatching(tableATuples.toIndexedSeq,tableBTuples.toIndexedSeq)
          finalMatching ++= matchingForGroup
        }
      }
    }}
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
