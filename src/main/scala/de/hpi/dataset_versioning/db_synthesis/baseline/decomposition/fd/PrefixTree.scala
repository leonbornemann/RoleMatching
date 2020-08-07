package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.fd

import scala.collection.mutable

class PrefixTree() {

  def putAll(toNewlyAdd: mutable.HashMap[collection.IndexedSeq[Int], collection.IndexedSeq[Int]]) = {
    toNewlyAdd.foreach{case (lhs,rhs) => root.putAll(lhs,rhs)}
  }


  private val emptySetResult = mutable.HashSet[Int]()
  val root = new PrefixTreeNode[Int,Int]()

  def findBestOverlap(fd: (collection.IndexedSeq[Int], collection.IndexedSeq[Int])) = {
    //find cover set
    val (leftSide,rightSide) = fd
    val rightSideSet = rightSide.toSet
    val curRightSideCover = mutable.HashSet[Int]()
    //TODO: we can work in early abort
    for(leftIndexBorder <- 0 until leftSide.size){
      for( rightIndexBorder <- (leftIndexBorder+1) to leftSide.size){
        //check what we have
        val toCheck = leftSide.slice(leftIndexBorder,rightIndexBorder)
        val storedRightSide = root.get(toCheck).getOrElse(emptySetResult)
        curRightSideCover ++= storedRightSide.intersect(rightSideSet)
      }
    }
    if(!curRightSideCover.isEmpty){
      Some(leftSide,curRightSideCover.toIndexedSeq.sorted)
    } else
      None
  }

  def fdLHSUnion(lhs: collection.IndexedSeq[Int], otherLHS: collection.IndexedSeq[Int]): collection.IndexedSeq[Int] = {
    (lhs ++ otherLHS).toSet.toIndexedSeq.sorted
  }

  def intersectFDs(toIntersectWith: collection.Map[collection.IndexedSeq[Int], collection.IndexedSeq[Int]]):collection.Map[collection.IndexedSeq[Int], collection.IndexedSeq[Int]] = {
    val intersection = mutable.HashMap[collection.IndexedSeq[Int], collection.IndexedSeq[Int]]()
    //first pass: exact matches
    toIntersectWith.foreach(fd => {
      val elem = root.get(fd._1)
      if(elem.isDefined && root.get(fd._1).get == fd._2.toSet){
        intersection += fd
      }
    })
    //second pass: toIntersectWith has a larger LHS
    toIntersectWith.foreach(fd => {
      if(!intersection.contains(fd._1)){
        val overlap = findBestOverlap(fd)
        if(overlap.isDefined)
          intersection += overlap.get
      }
    })
    //third pass: build Prefix Tree for right side and check left side!
    val rightAsPrefixTree = new PrefixTree()
    rightAsPrefixTree.initializeFDSet(toIntersectWith)
    root.foreach(fd => {
      if(!intersection.contains(fd._1)){
        val overlap = rightAsPrefixTree.findBestOverlap(fd)
        if(overlap.isDefined) {
          intersection += overlap.get
        }
      }
    })
    /**
     * Now deal with the case:
     * V1: A -> C
     * V2: B -> C
     * Intersection: AB -> C
     */
    val byRHS = toIntersectWith
      .toIndexedSeq
      .flatMap{case (l,r) => r.map(rhsElem => (l,rhsElem))}
      .groupMap(k => k._2)(k => k._1)
    val toNewlyAdd = mutable.HashMap[collection.IndexedSeq[Int],collection.IndexedSeq[Int]]()
    root.foreach{case (lhs,rhs) => {
      rhs.foreach(rhsElem => {
        val otherLHSCollection = byRHS.getOrElse(rhsElem,mutable.IndexedSeq())
        otherLHSCollection.foreach(otherLHS => {
          toNewlyAdd.put(fdLHSUnion(lhs,otherLHS),mutable.IndexedSeq(rhsElem))
        })
      })
    }}
    val curIntersectionAsPRefixTree = new PrefixTree()
    curIntersectionAsPRefixTree.initializeFDSet(intersection)
    curIntersectionAsPRefixTree.putAll(toNewlyAdd)
    curIntersectionAsPRefixTree.root.toMap
    //TODO: test this method
    //TODO: is the resulting FD prefix tree still minimal?
  }


  def initializeFDSet(fds: collection.Map[collection.IndexedSeq[Int], collection.IndexedSeq[Int]]) = {
    fds.foreach{case (left,right) => root.putAll(left,right)}
  }

}
