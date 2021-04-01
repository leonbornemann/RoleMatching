package de.hpi.tfm.compatibility.graph.fact.internal

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.{FactMatchCreator, TupleReference}
import de.hpi.tfm.compatibility.index.TupleSetIndex

class InternalFactMatchGraphCreator[A](tuples: IndexedSeq[TupleReference[A]],graphConfig:GraphConfig) extends FactMatchCreator[A] with StrictLogging{

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
          facts.add(edge.get)
      }
    }
  }

  def gaussSum(n: Int) = n*n+1/2

  def squareProductTooBig(n:Int): Boolean = {
    if(gaussSum(n) > 50) true else false
  }

  override def getGraphConfig: GraphConfig = graphConfig
}
object InternalFactMatchGraphCreator{



}
