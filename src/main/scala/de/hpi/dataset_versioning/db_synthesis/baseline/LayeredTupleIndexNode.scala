package de.hpi.dataset_versioning.db_synthesis.baseline

import java.time.LocalDate

import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.TemporalDatabaseTableTrait

import scala.collection.mutable.ArrayBuffer

class LayeredTupleIndexNode[A](_isLeafNode:Boolean) extends Iterable[Iterable[(TemporalDatabaseTableTrait[A],Int)]]{

  def insert(table: TemporalDatabaseTableTrait[A], rowIndex: Int,colIndex:Int, chosenTimestamps: ArrayBuffer[LocalDate]):Unit = {
    if(chosenTimestamps.isEmpty)
      addTupleReference(table,rowIndex)
    else{
      val ts = chosenTimestamps.head
      if(!table.fieldIsWildcardAt(rowIndex, colIndex, ts)){
        val valueAtTimestamp = table.fieldValueAtTimestamp(rowIndex, colIndex, ts)
        val childNode = children.getOrElseUpdate(valueAtTimestamp, createNewChildNode(chosenTimestamps))
        childNode.insert(table,rowIndex,colIndex,chosenTimestamps.tail)
      } else{
        containedWildcardTuples.get.addOne((table,rowIndex,colIndex,chosenTimestamps.tail))
        children.values.foreach(c => c.insert(table,rowIndex,colIndex,chosenTimestamps.tail))
      }
    }
  }


  private def createNewChildNode(chosenTimestamps: ArrayBuffer[LocalDate]) = {
    val newNode = new LayeredTupleIndexNode[A](chosenTimestamps.size == 1)
    //add stored wildcards:
    if(containedWildcardTuples.get.size>0)
      println()
    containedWildcardTuples.get.foreach(t => newNode.insert(t._1,t._2,t._3,t._4))
    newNode
  }

  def isLeafNode = containedTuples.isDefined

  def addTupleReference(table: TemporalDatabaseTableTrait[A], rowIndex: Int) = {
    containedTuples.get.addOne((table,rowIndex))
  }

  val containedTuples = if(_isLeafNode) Some(collection.mutable.ArrayBuffer[(TemporalDatabaseTableTrait[A],Int)]()) else None
  val children = collection.mutable.HashMap[A,LayeredTupleIndexNode[A]]()
  val containedWildcardTuples = if(!_isLeafNode) Some(collection.mutable.ArrayBuffer[(TemporalDatabaseTableTrait[A],Int,Int,ArrayBuffer[LocalDate])]()) else None

  override def iterator: Iterator[Iterable[(TemporalDatabaseTableTrait[A], Int)]] = new LeafNodeIterator()

  class LeafNodeIterator() extends Iterator[Iterable[(TemporalDatabaseTableTrait[A], Int)]]{
    if(isLeafNode)
      assert(children.isEmpty)
    var _hasNext = true
    var curChildrenIterator = children.iterator
    var curChildIterator:Iterator[Iterable[(TemporalDatabaseTableTrait[A], Int)]] = null
    if(!isLeafNode && !curChildrenIterator.hasNext)
      _hasNext = false

    override def hasNext: Boolean = _hasNext

    override def next(): Iterable[(TemporalDatabaseTableTrait[A], Int)] = {
      if(!hasNext)
        throw new NoSuchElementException
      else if(isLeafNode) {
        _hasNext=false
        containedTuples.get
      } else {
        if(curChildIterator==null || !curChildIterator.hasNext)
          curChildIterator = curChildrenIterator.next()._2.iterator
        assert(curChildIterator.hasNext) //we assume that we always have at least one child with content, otherwise this node should not even exist
        val toReturn = curChildIterator.next()
        _hasNext = curChildIterator.hasNext || curChildrenIterator.hasNext
        toReturn
      }
    }
  }

}
