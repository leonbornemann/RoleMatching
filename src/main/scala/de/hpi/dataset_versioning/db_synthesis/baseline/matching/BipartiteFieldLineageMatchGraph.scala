package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.db_synthesis.baseline.index.{BipartiteTupleGroup, BipartiteTupleIndex, TupleSetIndex}

import java.time.LocalDate
import scala.collection.mutable

class BipartiteFieldLineageMatchGraph[A](tuplesLeft: IndexedSeq[TupleReference[A]],tuplesRight: IndexedSeq[TupleReference[A]]) extends FieldLineageGraph[A]{

  init()

  def init() = {
    val index = new BipartiteTupleIndex[A](tuplesLeft,tuplesRight,IndexedSeq(),IndexedSeq(),true)
    buildGraph(tuplesLeft,tuplesRight,index)
  }

  def productTooBig(size: Int, size1: Int): Boolean = {
    size*size1>50
  }

  def buildGraph(originalInputLeft:IndexedSeq[TupleReference[A]], originalInputRight:IndexedSeq[TupleReference[A]], index: BipartiteTupleIndex[A]):Unit = {
    if(!index.indexFailed){
      index.getBipartiteTupleGroupIterator().foreach{case g => {
        val tuplesLeft = g.tuplesLeft
        val tuplesRight = g.tuplesRight
        buildGraphRecursively(g.chosenTimestamps.toIndexedSeq,g.valuesAtTimestamps, tuplesLeft, tuplesRight)
        //TODO: process Wildcards to others:
        buildGraphRecursively(g.chosenTimestamps.toIndexedSeq,g.valuesAtTimestamps,g.wildcardTuplesLeft,g.tuplesRight)
        buildGraphRecursively(g.chosenTimestamps.toIndexedSeq,g.valuesAtTimestamps,g.tuplesLeft,g.wildcardTuplesRight)
        //Wildcards to Wildcards:
        buildGraphRecursively(g.chosenTimestamps.toIndexedSeq,g.valuesAtTimestamps,g.wildcardTuplesLeft,g.wildcardTuplesRight)
      }}
    } else {
      doPairwiseMatching(originalInputLeft,originalInputRight)
    }
  }

  private def buildGraphRecursively(parentTimestamps:IndexedSeq[LocalDate], parentValues:IndexedSeq[A], tuplesLeft: IndexedSeq[TupleReference[A]], tuplesRight: IndexedSeq[TupleReference[A]]) = {
    if (productTooBig(tuplesLeft.size, tuplesRight.size)) {
      //further index this: new Index
      val newIndexForSubNode = new BipartiteTupleIndex[A](tuplesLeft, tuplesRight, parentTimestamps, parentValues, true)
      buildGraph(tuplesLeft, tuplesRight, newIndexForSubNode)
    } else {
      doPairwiseMatching(tuplesLeft, tuplesRight)
    }
  }

  private def doPairwiseMatching(tuplesLeft: IndexedSeq[TupleReference[A]], tuplesRight:IndexedSeq[TupleReference[A]]) = {
    //we construct a graph as an adjacency list:
    //pairwise matching to find out the edge-weights:
    if(tuplesLeft.size>0 && tuplesRight.size>0) {
      for (i <- 0 until tuplesLeft.size) {
        for (j <- 0 until tuplesRight.size) {
          val ref1 = tuplesLeft(i)
          val ref2 = tuplesRight(j)
          val edge = getTupleMatchOption(ref1, ref2)
          if (edge.isDefined)
            edges.add(edge.get)
        }
      }
    }
  }

}
