import de.hpi.socrata.change.temporal_tables.time.{TimeInterval, TimeIntervalSequence}

import java.time.LocalDate

object TimeIntervalSequenceTest extends App {

  def buildIntervalSequence(value: IndexedSeq[(Int, Int)]) = {
    val sortedTimeIntervals = value.map{case (s,e) => TimeInterval(LocalDate.ofEpochDay(s),if(e==Integer.MAX_VALUE) None else Some(LocalDate.ofEpochDay(e)))}
    TimeIntervalSequence(sortedTimeIntervals)
  }

  def unionTest() = {
    var is1 = buildIntervalSequence(IndexedSeq((1,100)))
    var is2 = buildIntervalSequence(IndexedSeq((1,100)))
    assert(is1.union(is2) == is1)
    assert(is2.union(is1) == is1)
    is2 = buildIntervalSequence(IndexedSeq((1,2),(100,100)))
    assert(is1.union(is2) == is1)
    assert(is2.union(is1) == is1)
    is2 = buildIntervalSequence(IndexedSeq((101,200)))
    assert(is1.union(is2) == buildIntervalSequence(IndexedSeq((1,200))))
    assert(is2.union(is1) == buildIntervalSequence(IndexedSeq((1,200))))
    is2 = buildIntervalSequence(IndexedSeq((99,Integer.MAX_VALUE)))
    assert(is1.union(is2) == buildIntervalSequence(IndexedSeq((1,Integer.MAX_VALUE))))
    assert(is2.union(is1) == buildIntervalSequence(IndexedSeq((1,Integer.MAX_VALUE))))
    is2 = buildIntervalSequence(IndexedSeq((102,200)))
    assert(is1.union(is2) == buildIntervalSequence(IndexedSeq((1,100),(102,200))))
    assert(is2.union(is1) == buildIntervalSequence(IndexedSeq((1,100),(102,200))))
    //more complicate test:
    is1 = buildIntervalSequence(IndexedSeq((1,4),(9,11),(14,14),(17,20),(22,24)))
    is2 = buildIntervalSequence(IndexedSeq((2,5),(7,10),(13,15),(21,21),(23,26)))
    val expectedResult = buildIntervalSequence(IndexedSeq((1, 5), (7, 11), (13, 15), (17, 26)))
    assert(is1.union(is2) == expectedResult)
    assert(is2.union(is1) == expectedResult)
  }

  unionTest()

  def intersectionTest() = {
    var is1 = buildIntervalSequence(IndexedSeq((1,100)))
    var is2 = buildIntervalSequence(IndexedSeq((1,100)))
    assert(is1.intersect(is2) == is1)
    assert(is2.intersect(is1) == is1)
    is1 = buildIntervalSequence(IndexedSeq((1,100)))
    is2 = buildIntervalSequence(IndexedSeq((101,200)))
    assert(is1.intersect(is2).isEmpty)
    assert(is2.intersect(is1).isEmpty)
    is2 = buildIntervalSequence(IndexedSeq((0,101),(400,600),(800,1200)))
    assert(is1.intersect(is2) == is1)
    assert(is2.intersect(is1) == is1)
    is2 = buildIntervalSequence(IndexedSeq((0,20),(40,60),(80,120)))
    var expectedResult = buildIntervalSequence(IndexedSeq((1, 20), (40, 60), (80, 100)))
    assert(is1.intersect(is2) == expectedResult)
    assert(is2.intersect(is1) == expectedResult)
    is1 = buildIntervalSequence(IndexedSeq((1,4),(9,11),(14,14),(17,20),(22,30),(34,Integer.MAX_VALUE)))
    is2 = buildIntervalSequence(IndexedSeq((2,5),(7,10),(13,18),(21,24),(27,35)))
    expectedResult = buildIntervalSequence(IndexedSeq((2, 4), (9, 10), (14, 14),(17, 18),(22, 24),(27, 30),(34,35)))
    assert(is1.intersect(is2) == expectedResult)
    assert(is2.intersect(is1) == expectedResult)
    //weiter testen!
    is1 = buildIntervalSequence(IndexedSeq((50,Integer.MAX_VALUE)))
    is2 = buildIntervalSequence(IndexedSeq((1,Integer.MAX_VALUE)))
    expectedResult = buildIntervalSequence(IndexedSeq((50, Integer.MAX_VALUE)))
    assert(is1.intersect(is2) == is1)
    assert(is2.intersect(is1) == is1)
  }

  intersectionTest()
}
