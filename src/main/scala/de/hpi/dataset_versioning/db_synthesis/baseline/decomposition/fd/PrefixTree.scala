package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.fd

import scala.collection.mutable

class PrefixTree() {
  def put(fd: (collection.IndexedSeq[Int], collection.IndexedSeq[Int])) ={
    val (lhs,rhs) = fd
    root.putAll(lhs,rhs)
  }


  def putAll(toNewlyAdd: collection.Iterable[(collection.IndexedSeq[Int], collection.IndexedSeq[Int])]) = {
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

  def intersectFDs(toIntersectWith: collection.Map[collection.IndexedSeq[Int], collection.IndexedSeq[Int]],
                  maxFDSizeForUnion:Int
                  ):collection.Map[collection.IndexedSeq[Int], collection.IndexedSeq[Int]] = {
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
    val onlyOneOnRHS = toIntersectWith
      .toIndexedSeq
      .flatMap{case (l,r) => r.map(rhsElem => (l,rhsElem))}
    val byRHS = onlyOneOnRHS.groupMap(k => k._2)(k => k._1)
//    val tree1 = new PrefixTree().initializeFDSet(toIntersectWith)
//    val tree2 = new PrefixTree().initializeFDSet(onlyOneOnRHS.map(t => (t._1,mutable.IndexedSeq(t._2))))
//    assert(tree1.root.toIndexedSeq == tree2.root.toIndexedSeq)
    //val toNewlyAdd = mutable.ArrayBuffer[(collection.IndexedSeq[Int],collection.IndexedSeq[Int])]()
    val curIntersectionAsPRefixTree = new PrefixTree
    curIntersectionAsPRefixTree.initializeFDSet(intersection)
    root.foreach{case (lhs,rhs) => {
      rhs.foreach(rhsElem => {
        val otherLHSCollection = byRHS.getOrElse(rhsElem,mutable.IndexedSeq())
        otherLHSCollection.foreach(otherLHS => {
          //TODO: we need a lookup here if union AB -> C is found and B -> C is already in intersection, we dont want to add AB -> C
          //TODO: maybe use the logic in findBestOVerlap
          val curFD = (fdLHSUnion(lhs,otherLHS),mutable.IndexedSeq(rhsElem))
          if(!curIntersectionAsPRefixTree.findBestOverlap(curFD).isDefined && curFD._1.size<=maxFDSizeForUnion){ //IMPORTANT: THIS ONLY WORKS BECAUSE RHS of curFD is always of size 1
            curIntersectionAsPRefixTree.put(curFD)
          }
        })
      })
    }}
    curIntersectionAsPRefixTree.root.toMap
    //TODO: test this method
    //TODO: is the resulting FD prefix tree still minimal?
  }


  def initializeFDSet(fds: collection.Iterable[(collection.IndexedSeq[Int], collection.IndexedSeq[Int])]) = {
    fds.foreach{case (left,right) => {
      root.putAll(left,right)
    }}
    this
  }

}
