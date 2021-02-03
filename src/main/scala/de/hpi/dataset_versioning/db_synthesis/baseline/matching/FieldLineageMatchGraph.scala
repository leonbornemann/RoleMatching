package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.index.TupleSetIndex

import scala.collection.mutable

class FieldLineageMatchGraph[A](tuples: IndexedSeq[TupleReference[A]]) extends StrictLogging{

  val edges = mutable.HashSet[General_1_to_1_TupleMatching[A]]()
  init()

  def init() = {
    val index = new TupleSetIndex[A](tuples,IndexedSeq(),IndexedSeq(),tuples.head.table.wildcardValues.toSet,true)
    buildGraph(tuples,index)
  }

  def buildGraph(originalInput:IndexedSeq[TupleReference[A]], index: TupleSetIndex[A]):Unit = {
    if(index.indexBuildWasSuccessfull){
      index.tupleGroupIterator(true).foreach{case g => {
        val tuplesInNode = (g.tuplesInNode ++ g.wildcardTuples)
        if(squareProductTooBig(tuplesInNode.size)){
          //further index this: new Index
          val newIndexForSubNode = new TupleSetIndex[A](tuplesInNode.toIndexedSeq,index.indexedTimestamps.toIndexedSeq,g.valuesAtTimestamps,index.wildcardKeyValues,true)
          buildGraph(tuplesInNode.toIndexedSeq,newIndexForSubNode)
        } else{
          val tuplesInNodeAsIndexedSeq = tuplesInNode.toIndexedSeq
          doPairwiseMatching(tuplesInNodeAsIndexedSeq)
        }
      }}
    } else {
      doPairwiseMatching(originalInput)
    }
  }

  private def doPairwiseMatching(tuplesInNodeAsIndexedSeq: IndexedSeq[TupleReference[A]]) = {
    //we construct a graph as an adjacency list:
    //pairwise matching to find out the edge-weights:
    for (i <- 0 until tuplesInNodeAsIndexedSeq.size) {
      for (j <- i + 1 until tuplesInNodeAsIndexedSeq.size) {
        val ref1 = tuplesInNodeAsIndexedSeq(i)
        val ref2 = tuplesInNodeAsIndexedSeq(j)
        val edge = getTupleMatchOption(ref1, ref2)
        if (edge.isDefined)
          edges.add(edge.get)
      }
    }
  }

  private def getTupleMatchOption(ref1:TupleReference[A], ref2:TupleReference[A]) = {
    val mappedFieldLineages = buildTuples(ref1, ref2) // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
    val evidence = mappedFieldLineages._1.countOverlapEvidence(mappedFieldLineages._2)
    if (evidence == -1) {
      None
    } else {
      Some(General_1_to_1_TupleMatching(ref1,ref2, evidence))
    }
  }

  def gaussSum(n: Int) = n*n+1/2

  def squareProductTooBig(n:Int): Boolean = {
    if(gaussSum(n) > 50) true else false
  }

  def buildTuples(ref1: TupleReference[A], ref2: TupleReference[A]) = {
    val lineages1 = ref1.getDataTuple
    val lineages2 = ref2.getDataTuple
    assert(lineages1.size==1 && lineages2.size==1)
    (lineages1.head,lineages2.head)
  }

}
object FieldLineageMatchGraph{



}
