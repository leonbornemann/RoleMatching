package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.index.TupleSetIndex
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

import java.time.LocalDate
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
    val graphBasedTupleMapper = new GraphBasedTupleMapper(tuples,edges)
    graphBasedTupleMapper.mapGreedy()
  }

  def buildGraph(index: TupleSetIndex[A],edges:mutable.HashSet[General_1_to_1_TupleMatching[A]]):Unit = {
    index.tupleGroupIterator(true).foreach{case g => {
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
    val evidence = mappedFieldLineages._1.countOverlapEvidence(mappedFieldLineages._2)
//    val mergedTupleOptions = mergeTupleSketches(Map(mappedFieldLineages))
//    if(evidence!= -1 && !mergedTupleOptions.head.isDefined){
//      println()
//    }
    if (evidence == -1) {
      //illegalMatch - we do nothing
      None
    } else {
//      val mergedTuple = mergedTupleOptions.map(_.get)
//      val sizeAfterMerge = countChanges(mergedTuple, mergedInsertTime) //
//      val sizeBeforeMergeA = countChanges(originalTupleA, insertTimeA)
//      val sizeBeforeMergeB = countChanges(originalTupleB, insertTimeB)
//      val bestCaseChangeImprovement = sizeBeforeMergeA._1 + sizeBeforeMergeB._1 - sizeAfterMerge._1
//      if (bestCaseChangeImprovement < 0) {
//        //debug
//        println(ref1)
//        println(ref2)
//        println(originalTupleA)
//        println(originalTupleB)
//        println(mergedTuple)
//        assert(false)
//      }
      Some(General_1_to_1_TupleMatching(ref1,ref2, evidence))
    }
  }

  def buildTuples(ref1: TupleReference[A], ref2: TupleReference[A]) = {
    val lineages1 = ref1.getDataTuple
    val lineages2 = ref2.getDataTuple
    assert(lineages1.size==1 && lineages2.size==1)
    (lineages1.head,lineages2.head)
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
      GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countFieldChangesSimple(tuple)
    } else {
      ???
      //tuple.map(_.countChanges(insertTime, GLOBAL_CONFIG.CHANGE_COUNT_METHOD)).sum
    }
  }
}
object PairwiseTupleMapper extends StrictLogging{
  logger.debug("Current implementiation allows for negative change-gain when having evidence >=0 - we will need to see if we want this!")
}
