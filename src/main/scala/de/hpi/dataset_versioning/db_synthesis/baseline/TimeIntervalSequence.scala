package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.TimeInterval

import scala.collection.mutable.ArrayBuffer

class TimeIntervalSequence(val sortedTimeIntervals:ArrayBuffer[TimeInterval] = ArrayBuffer()) {

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

}
