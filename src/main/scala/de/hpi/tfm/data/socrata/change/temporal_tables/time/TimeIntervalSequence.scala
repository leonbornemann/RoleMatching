package de.hpi.tfm.data.socrata.change.temporal_tables.time

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

/**
 * No in-place modifications allowed - they will break the logic
 *
 * @param sortedTimeIntervals
 */
case class TimeIntervalSequence(val sortedTimeIntervals: collection.immutable.IndexedSeq[TimeInterval] = IndexedSeq()) {
  def contains(ld: LocalDate): Boolean = sortedTimeIntervals.exists(ti => ti.contains(ld))


  private def mergeIntoUnion(union: ArrayBuffer[TimeInterval], toMerge: TimeInterval) = {
    if (union.isEmpty)
      union.addOne(toMerge)
    else {
      val lastUnionElem = union.last
      assert(lastUnionElem <= toMerge)
      if (lastUnionElem.endOrMax.isBefore(toMerge.begin)) {
        if (lastUnionElem.endOrMax == toMerge.begin.minusDays(1)) {
          val mergedInterval = TimeInterval(lastUnionElem.begin, toMerge.`end`)
          union(union.size - 1) = mergedInterval
        } else
          union.addOne(toMerge)
      } else {
        //we have an overlap
        val mergedInterval = lastUnionElem.mergeWithOverlapping(toMerge)
        union(union.size - 1) = mergedInterval
      }
    }
  }

  def mergeIntoIntersection(intersection: ArrayBuffer[TimeInterval], intersectedInterval: Option[TimeInterval]) = {
    if (intersectedInterval.isDefined) {
      intersection.addOne(intersectedInterval.get)
    }
  }

  def intersect(other: TimeIntervalSequence) = {
    if (other.sortedTimeIntervals.isEmpty || this.sortedTimeIntervals.isEmpty)
      TimeIntervalSequence(IndexedSeq())
    else {
      val intersection: ArrayBuffer[TimeInterval] = ArrayBuffer()
      var otherIndex = 0
      var myIndex = 0
      while (myIndex < sortedTimeIntervals.size && otherIndex < other.sortedTimeIntervals.size) {
        //here we do the actual merging:
        val myHead = sortedTimeIntervals(myIndex)
        val otherHead = other.sortedTimeIntervals(otherIndex)
        if (myHead == otherHead) {
          mergeIntoIntersection(intersection, Some(myHead))
          myIndex += 1
          otherIndex += 1
        } else {
          val intersectedInterval = myHead.intersect(otherHead)
          mergeIntoIntersection(intersection, intersectedInterval)
          if (!intersectedInterval.isDefined) {
            //increment the index of the sequence containing the interval that starts earlier
            if (myHead.begin.isBefore(otherHead.begin)) myIndex += 1
            else otherIndex += 1
          } else {
            //increment the index of the sequence containing the inveral matching the intersection end (the earlier end), both if both are the same
            if (myHead.`end` == intersectedInterval.get.`end`) myIndex += 1
            if (otherHead.`end` == intersectedInterval.get.`end`) otherIndex += 1
          }
        }
      }
      TimeIntervalSequence(intersection.toIndexedSeq)
    }
  }

  def union(other: TimeIntervalSequence) = {
    if (sortedTimeIntervals.isEmpty)
      other
    else if (other.sortedTimeIntervals.isEmpty)
      this
    else {
      val union: ArrayBuffer[TimeInterval] = ArrayBuffer()
      var otherIndex = 0
      var myIndex = 0
      while (myIndex < sortedTimeIntervals.size || otherIndex < other.sortedTimeIntervals.size) {
        //TODO: merge myHead and OtherHead
        if (otherIndex >= other.sortedTimeIntervals.size) {
          sortedTimeIntervals.slice(myIndex, sortedTimeIntervals.size).foreach(ti => mergeIntoUnion(union, ti))
          myIndex = sortedTimeIntervals.size
        } else if (myIndex >= sortedTimeIntervals.size) {
          other.sortedTimeIntervals.slice(otherIndex, other.sortedTimeIntervals.size).foreach(ti => mergeIntoUnion(union, ti))
          otherIndex = other.sortedTimeIntervals.size
        } else {
          //here we do the actual merging:
          val myHead = sortedTimeIntervals(myIndex)
          val otherHead = other.sortedTimeIntervals(otherIndex)
          if (myHead == otherHead) {
            mergeIntoUnion(union, myHead)
            myIndex += 1
            otherIndex += 1
          } else if (myHead < otherHead) {
            mergeIntoUnion(union, myHead)
            myIndex += 1
          } else {
            mergeIntoUnion(union, otherHead)
            otherIndex += 1
          }
        }
      }
      TimeIntervalSequence(union.toIndexedSeq)
    }
  }

  //    def isIncludedIn(otherSequence: TimeIntervalSequence): Boolean = {
  //      val myIntervalIterator = sortedTimeIntervals.iterator
  //      val other = otherSequence.sortedTimeIntervals.iterator
  //      var included = true
  //      var curElemB = other.next()
  //      while(included && myIntervalIterator.hasNext){
  //        var curElemA = myIntervalIterator.next()
  //        while(!curElemA.begin.isBefore(curElemB.endOrMax) && other.hasNext){
  //          //the interval is too early
  //          curElemB = other.next()
  //        }
  //        included = curElemA.subIntervalOf(curElemB)
  //      }
  //      included
  //      TODO: rereead this method and check if it is correct - it is not, fix it if needed!
  //    }

  def isEmpty = sortedTimeIntervals.isEmpty

}
