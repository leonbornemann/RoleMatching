package de.hpi.tfm.compatibility.index

import de.hpi.tfm.compatibility.graph.fact
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalDatabaseTableTrait

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class LayeredTupleIndexNode[A](val key:Option[A], val parent:LayeredTupleIndexNode[A], _isLeafNode:Boolean) extends Iterable[(IndexedSeq[A],Iterable[TupleReference[A]])]{

  def insert(table: TemporalDatabaseTableTrait[A], rowIndex: Int,colIndex:Int, chosenTimestamps: ArrayBuffer[LocalDate]):Unit = {
    if(chosenTimestamps.isEmpty)
      addTupleReference(table,rowIndex)
    else{
      val ts = chosenTimestamps.head
      if(!table.fieldIsWildcardAt(rowIndex, colIndex, ts)){
        val valueAtTimestamp = table.fieldValueAtTimestamp(rowIndex, colIndex, ts)
        val childNode = children.getOrElseUpdate(valueAtTimestamp, createNewChildNode(valueAtTimestamp,this,chosenTimestamps))
        childNode.insert(table,rowIndex,colIndex,chosenTimestamps.tail)
      } else{
        containedWildcardTuples.get.addOne((table,rowIndex,colIndex,chosenTimestamps.tail))
        children.values.foreach(c => c.insert(table,rowIndex,colIndex,chosenTimestamps.tail))
      }
    }
  }

  private def createNewChildNode(value:A,parent:LayeredTupleIndexNode[A],chosenTimestamps: ArrayBuffer[LocalDate]) = {
    val newNode = new LayeredTupleIndexNode[A](Some(value),parent,chosenTimestamps.size == 1)
    //add stored wildcards:
    containedWildcardTuples.get.foreach(t => newNode.insert(t._1,t._2,t._3,t._4))
    newNode
  }

  def isLeafNode = containedTuples.isDefined

  def addTupleReference(table: TemporalDatabaseTableTrait[A], rowIndex: Int) = {
    containedTuples.get.addOne(fact.TupleReference(table,rowIndex))
  }

  val containedTuples = if(_isLeafNode) Some(collection.mutable.ArrayBuffer[TupleReference[A]]()) else None
  val children = collection.mutable.HashMap[A,LayeredTupleIndexNode[A]]()
  val containedWildcardTuples = if(!_isLeafNode) Some(collection.mutable.ArrayBuffer[(TemporalDatabaseTableTrait[A],Int,Int,ArrayBuffer[LocalDate])]()) else None

  def allKeys: collection.IndexedSeq[A] = {
    val curKeys = ArrayBuffer[A]()
    var curNode = this
    while(!curNode.key.isEmpty){
      curKeys.append(curNode.key.get)
      curNode = curNode.parent
    }
    curKeys.reverse
  }

  override def iterator: Iterator[(IndexedSeq[A],Iterable[TupleReference[A]])] = new LeafNodeIterator()

  class LeafNodeIterator() extends Iterator[(IndexedSeq[A],Iterable[TupleReference[A]])]{
    if(isLeafNode)
      assert(children.isEmpty)
    var _hasNext = true
    var curChildrenIterator = children.iterator
    var curChildIterator:Iterator[(IndexedSeq[A],Iterable[TupleReference[A]])] = null
    if(!isLeafNode && !curChildrenIterator.hasNext)
      _hasNext = false

    override def hasNext: Boolean = _hasNext

    override def next(): (IndexedSeq[A],Iterable[TupleReference[A]]) = {
      if(!hasNext)
        throw new NoSuchElementException
      else if(isLeafNode) {
        _hasNext=false
        (allKeys.toIndexedSeq,containedTuples.get)
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
