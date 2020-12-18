package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait

import scala.collection.mutable.ArrayBuffer

class TupleSetMatching[A](val tableA: TemporalDatabaseTableTrait[A],
                          val tableB: TemporalDatabaseTableTrait[A],
                          val matchedTuples: ArrayBuffer[General_Many_To_Many_TupleMatching[A]] = ArrayBuffer[General_Many_To_Many_TupleMatching[A]]()) {

  implicit class TuppleAdd(t: (Int, Int)) {
    def +(p: (Int, Int)) = (p._1 + t._1, p._2 + t._2)
    def -(p: (Int, Int)) = (p._1 - t._1, p._2 - t._2)
  }

  def totalEvidence = matchedTuples.map(_.evidence).sum

  def totalChangeBenefit = {
    val before = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(tableA) +
      GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(tableB)
    val after = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(matchedTuples.map(_.changeRange))
    before-after
  }

}
object TupleSetMatching extends StrictLogging{
}
