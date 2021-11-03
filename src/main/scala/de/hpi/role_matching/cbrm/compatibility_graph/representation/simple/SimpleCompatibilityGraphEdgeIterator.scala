package de.hpi.role_matching.cbrm.compatibility_graph.representation.simple

import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.MemoryEfficientCompatiblityGraphWithoutEdgeWeight

class SimpleCompatibilityGraphEdgeIterator(graph: MemoryEfficientCompatiblityGraphWithoutEdgeWeight) extends Iterator[(Int,Int,SimpleCompatbilityGraphEdge,Seq[Boolean])]{

  val vertexIterator = graph.adjacencyList.iterator
  var curFirstNode:Int = -1
  var curAdjIterator:Iterator[(Int, Seq[Boolean])] = Iterator()
  var curEdgeOption:Option[(Int,Int,SimpleCompatbilityGraphEdge,Seq[Boolean])] = None
  advanceCurEdge()

  def advanceCurEdge() = {
    while(!curAdjIterator.hasNext && vertexIterator.hasNext){
      val (first,curAdjList) = vertexIterator.next
      curFirstNode = first
      curAdjIterator = curAdjList.iterator
    }
    if(!curAdjIterator.hasNext){
      curEdgeOption = None
    } else {
      assert(curAdjIterator.hasNext)
      val edge = curAdjIterator.next()
      val curSecondNode = edge._1
      val isEdgeForTrainDateEnd = edge._2
      curEdgeOption = Some((curFirstNode,curSecondNode,simple.SimpleCompatbilityGraphEdge(graph.getLineage(curFirstNode),graph.getLineage(curSecondNode)),isEdgeForTrainDateEnd))
    }
  }

  override def hasNext: Boolean = curEdgeOption.isDefined

  override def next(): (Int,Int,SimpleCompatbilityGraphEdge,Seq[Boolean]) = {
    val cur = curEdgeOption
    advanceCurEdge()
    cur.get
  }
}
