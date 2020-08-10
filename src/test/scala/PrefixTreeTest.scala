

import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.fd.PrefixTree

import scala.collection.mutable

object PrefixTreeTest extends App {
  testIterator
  testIntersection
  testPutExistingInfo

  def testPutExistingInfo = {
    var tree = new PrefixTree
    var initialFds = IndexedSeq[(IndexedSeq[Int], IndexedSeq[Int])](
      IndexedSeq(1) -> IndexedSeq(4),
      IndexedSeq(1,2) -> IndexedSeq(5)
    )
    var toPut = IndexedSeq[(IndexedSeq[Int], IndexedSeq[Int])](
      IndexedSeq(1,2,3) -> IndexedSeq(4,5)
    )
    tree.initializeFDSet(initialFds.toMap)
    tree.root.putAll(toPut(0)._1,toPut(0)._2)
    assert(tree.root.toIndexedSeq == initialFds)
  }


  private def testIntersection = {
    var tree = new PrefixTree
    var initialFds = IndexedSeq[(IndexedSeq[Int], IndexedSeq[Int])](
      IndexedSeq(1) -> IndexedSeq(6),
      IndexedSeq(2) -> IndexedSeq(3),
      IndexedSeq(11, 12, 13) -> IndexedSeq(14, 15)
    )
    var fdsToIntersect = IndexedSeq[(IndexedSeq[Int], IndexedSeq[Int])](
      IndexedSeq(1, 2, 3, 4, 5) -> IndexedSeq(6),
      IndexedSeq(2) -> IndexedSeq(3),
      IndexedSeq(11, 12) -> IndexedSeq(14, 15)
    )
    var expectedResult = Map[IndexedSeq[Int], IndexedSeq[Int]](
      IndexedSeq(1, 2, 3, 4, 5) -> IndexedSeq(6),
      IndexedSeq(2) -> IndexedSeq(3),
      IndexedSeq(11, 12, 13) -> IndexedSeq(14, 15)
    )
    tree.initializeFDSet(initialFds.toMap)
    var intersection = tree.intersectFDs(fdsToIntersect.toMap,5)
    assert(intersection == expectedResult)
    //more difficult cases:
    tree = new PrefixTree
    initialFds = IndexedSeq[(IndexedSeq[Int], IndexedSeq[Int])](
      IndexedSeq(2) -> IndexedSeq(3, 4),
      IndexedSeq(12) -> IndexedSeq(13, 14, 15, 16),
      IndexedSeq(21,22) -> IndexedSeq(23, 24),
      IndexedSeq(31,32) -> IndexedSeq(35,36),
    )
    fdsToIntersect = IndexedSeq[(IndexedSeq[Int], IndexedSeq[Int])](
      IndexedSeq(1,2) -> IndexedSeq(3, 4),
      IndexedSeq(11,12) -> IndexedSeq(15,16),
      IndexedSeq(22) -> IndexedSeq(23, 24),
      IndexedSeq(32) -> IndexedSeq(33, 34, 35, 36)
    )
    expectedResult = Map[IndexedSeq[Int], IndexedSeq[Int]](
      IndexedSeq(1, 2) -> IndexedSeq(3, 4),
      IndexedSeq(11,12) -> IndexedSeq(15,16),
      IndexedSeq(21,22) -> IndexedSeq(23, 24),
      IndexedSeq(31,32) -> IndexedSeq(35,36)
    )
    tree.initializeFDSet(initialFds.toMap)
    intersection = tree.intersectFDs(fdsToIntersect.toMap,5)
    assert(intersection == expectedResult)
    //TODO: test A->B C-> B ==> AC->B
    initialFds = IndexedSeq[(IndexedSeq[Int], IndexedSeq[Int])](
      IndexedSeq(1) -> IndexedSeq(3)
    )
    fdsToIntersect = IndexedSeq[(IndexedSeq[Int], IndexedSeq[Int])](
      IndexedSeq(2) -> IndexedSeq(3)
    )
    expectedResult = Map[IndexedSeq[Int], IndexedSeq[Int]](
      IndexedSeq(1, 2) -> IndexedSeq(3)
    )
    tree = new PrefixTree()
    tree.initializeFDSet(initialFds.toMap)
    intersection = tree.intersectFDs(fdsToIntersect.toMap,5)
    assert(intersection == expectedResult)
  }

  private def testIterator = {
    val tree1 = new PrefixTree()
    val fds = IndexedSeq[(IndexedSeq[Int], IndexedSeq[Int])](
      IndexedSeq(1) -> IndexedSeq(2),
      IndexedSeq(1, 2) -> IndexedSeq(3),
      IndexedSeq(1, 2, 3) -> IndexedSeq(4),
      IndexedSeq(1, 2, 4) -> IndexedSeq(5),
      IndexedSeq(1, 2, 5) -> IndexedSeq(6),
      IndexedSeq(2) -> IndexedSeq(7),
      IndexedSeq(2, 3) -> IndexedSeq(8),
      IndexedSeq(2, 4) -> IndexedSeq(9),
      IndexedSeq(3, 4, 5, 6) -> IndexedSeq(10),
    )
    tree1.initializeFDSet(fds.toMap)
    assert(IndexedSeq() ++ tree1.root.iterator == fds)
    tree1.root.put(IndexedSeq(2, 3), 6)
    assert(tree1.root.get(IndexedSeq(2, 3)).get == Set(6, 8))
    val tree2 = new PrefixTree()
    assert(!tree2.root.iterator.hasNext)
    tree2.root.put(IndexedSeq(1), 2)
    assert(tree2.root.iterator.size == 1)
  }
}
