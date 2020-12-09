package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait

import scala.collection.mutable.ArrayBuffer

class TupleSetMatching[A](val tableA: TemporalDatabaseTableTrait[A],
                          val tableB: TemporalDatabaseTableTrait[A],
                          val matchedTuples: ArrayBuffer[General_Many_To_Many_TupleMatching[A]] = ArrayBuffer[General_Many_To_Many_TupleMatching[A]]()) {

  def unmatchedTupleIndices(left: TemporalDatabaseTableTrait[A]) = {

  }


  def totalScore = matchedTuples.map(_.score).sum

}
object TupleSetMatching extends StrictLogging{
}
