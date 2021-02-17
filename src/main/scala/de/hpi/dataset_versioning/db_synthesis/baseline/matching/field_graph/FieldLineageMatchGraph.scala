package de.hpi.dataset_versioning.db_synthesis.baseline.matching.field_graph

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.index.TupleSetIndex
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

class FieldLineageMatchGraph[A](tuples: IndexedSeq[TupleReference[A]]) extends FieldLineageGraph[A] with StrictLogging{


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

  def gaussSum(n: Int) = n*n+1/2

  def squareProductTooBig(n:Int): Boolean = {
    if(gaussSum(n) > 50) true else false
  }


}
object FieldLineageMatchGraph{



}
