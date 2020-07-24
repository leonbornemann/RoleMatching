package de.hpi.dataset_versioning.db_synthesis.top_down

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PrefixTreeNode[A:Ordering,B:Ordering]() extends Iterable[(collection.IndexedSeq[A],collection.IndexedSeq[B])]{

  def putAll(inputKey: collection.IndexedSeq[A], values: collection.IndexedSeq[B]):Unit = {
    val key = inputKey.sorted
    if(key.size==0)
      addToValueSet(values)
    else
      suffixes.getOrElseUpdate(key.head,new PrefixTreeNode[A,B]())
        .putAll(key.tail,values)
  }


  def get(inputKey: collection.IndexedSeq[A]):Option[mutable.HashSet[B]] = {
    val key = inputKey.sorted
    if(key.size==0)
      value
    else if(suffixes.contains(key.head))
      suffixes(key.head).get(key.tail)
    else
      None
  }

  def addToValueSet(value: B): Unit = {
    if(this.value.isDefined)
      this.value.get.add(value)
    else
      this.value = Some(mutable.HashSet[B](value))
  }

  def addToValueSet(values: collection.IndexedSeq[B]): Unit = {
    if(this.value.isDefined)
      this.value.get.addAll(values)
    else {
      this.value = Some(mutable.HashSet[B]())
      this.value.get.addAll(values)
    }
  }

  def put(inputKey: collection.IndexedSeq[A], value: B):Unit = {
    val key = inputKey.sorted
    if(key.size==0)
      addToValueSet(value)
    else
      suffixes.getOrElseUpdate(key.head,new PrefixTreeNode[A,B]())
        .put(key.tail,value)
  }

  var value:Option[mutable.HashSet[B]] = None
  val suffixes = mutable.TreeMap[A,PrefixTreeNode[A,B]]()

  override def iterator: Iterator[(collection.IndexedSeq[A], collection.IndexedSeq[B])] = {
    new PrefixTreeNodeIterator(this,mutable.IndexedSeq())
  }

  class PrefixTreeNodeIterator(root: PrefixTreeNode[A, B],keyPrefix:collection.IndexedSeq[A]) extends Iterator[(collection.IndexedSeq[A], collection.IndexedSeq[B])]{
    var curElemToReturn:(collection.IndexedSeq[A],collection.IndexedSeq[B]) = (null,null)
    if(root.value.isDefined)
      curElemToReturn = (keyPrefix,root.value.get.toIndexedSeq.sorted)
    val childIterator = root.suffixes.iterator
    var curIterator:Option[PrefixTreeNodeIterator] = None
    var done = false

    if(curElemToReturn==(null,null))
      jumpToNextReturnableElem()

    def jumpOneStep() = {
      if ((!curIterator.isDefined || !curIterator.get.hasNext) && !childIterator.hasNext){
        done = true
      } else if(!curIterator.isDefined || !curIterator.get.hasNext){
        val (key,child) = childIterator.next()
        curIterator = Some(new PrefixTreeNodeIterator(child,keyPrefix ++Seq(key)))
      } else{
        curElemToReturn = curIterator.get.next()
      }
    }

    def jumpToNextReturnableElem() = {
      assert(curElemToReturn == (null,null))
      jumpOneStep()
      //TODO: implement jumponestep and hasElemAtCurrent
      while(curElemToReturn == (null,null) && !done){
        jumpOneStep()
      }
    }

    override def next(): (collection.IndexedSeq[A], collection.IndexedSeq[B]) = {
      if(done)
        throw new NoSuchElementException()
      val toReturn = curElemToReturn
      curElemToReturn = (null,null)
      jumpToNextReturnableElem()
      toReturn
    }

    override def hasNext: Boolean = !done
  }

}
