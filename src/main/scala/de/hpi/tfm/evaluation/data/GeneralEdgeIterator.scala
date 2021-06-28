package de.hpi.tfm.evaluation.data

class GeneralEdgeIterator(graph: SlimGraphWithoutWeight) extends Iterator[(Int,Int,GeneralEdge,Seq[Boolean])]{

  val vertexIterator = graph.adjacencyList.iterator
  var curFirstNode:Int = -1
  var curAdjIterator:Iterator[(Int, Seq[Boolean])] = Iterator()
  var curEdgeOption = getCurNextEdge()

  def getCurNextEdge() = {
    while(!curAdjIterator.hasNext && vertexIterator.hasNext){
      val (first,curAdjList) = vertexIterator.next
      curFirstNode = first
      curAdjIterator = curAdjList.iterator
    }
    if(!curAdjIterator.hasNext){
      None
    } else {
      assert(curAdjIterator.hasNext)
      val edge = curAdjIterator.next()
      val curSecondNode = edge._1
      val isEdgeForTrainDateEnd = edge._2
      Some((curFirstNode,curSecondNode,GeneralEdge(graph.getLineage(curFirstNode),graph.getLineage(curSecondNode)),isEdgeForTrainDateEnd))
    }
  }

  override def hasNext: Boolean = curEdgeOption.isDefined

  override def next(): (Int,Int,GeneralEdge,Seq[Boolean]) = curEdgeOption.get
}
