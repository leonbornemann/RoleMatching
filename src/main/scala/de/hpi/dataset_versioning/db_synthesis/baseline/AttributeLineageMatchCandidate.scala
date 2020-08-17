package de.hpi.dataset_versioning.db_synthesis.baseline

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, TimeInterval}

import scala.collection.mutable

case class AttributeLineageMatchCandidate(al:AttributeLineage,
                                          matches:collection.mutable.HashMap[TimeInterval,AttributeLineage] = new mutable.HashMap()) {


  def unmatchedTimeIntervals = {
    val timeIntervalsSorted = matches.keys.toIndexedSeq.sortBy(_.begin.toEpochDay)
    val unmatchedTimeIntervals = collection.mutable.ArrayBuffer[TimeInterval]()
    unmatchedTimeIntervals.addOne(TimeInterval(LocalDate.MIN,Some(timeIntervalsSorted.head.begin.minusDays(1))))
    for(i <- 1 until timeIntervalsSorted.size){
      val start = timeIntervalsSorted(i-1).`end`.get
      val end = timeIntervalsSorted(i).begin.minusDays(1)
      assert(!start.isBefore(end))
      unmatchedTimeIntervals.addOne(TimeInterval(start,Some(end)))
    }
    TimeIntervalSequence(unmatchedTimeIntervals.toIndexedSeq)
    //TODO
  }

}
