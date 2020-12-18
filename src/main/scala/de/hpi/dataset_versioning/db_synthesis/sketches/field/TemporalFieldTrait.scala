package de.hpi.dataset_versioning.db_synthesis.sketches.field

import java.time.LocalDate
import de.hpi.dataset_versioning.data.change.temporal_tables.time.{TimeInterval, TimeIntervalSequence}
import de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based.{FieldChangeCounter, UpdateChangeCounter}

import scala.collection.mutable

trait TemporalFieldTrait[T] {

  def valueAt(ts: LocalDate): T

  def allTimestamps: Iterable[LocalDate] = getValueLineage.keySet

  def allNonWildcardTimestamps: Iterable[LocalDate] = {
    getValueLineage
      .filter(t => !isWildcard(t._2))
      .keySet
  }

  def numValues:Int

  def isRowDelete(a: T) :Boolean

  def isWildcard(a: T) :Boolean

  def countChanges(changeCounter:FieldChangeCounter):(Int,Int)

  def nonWildCardValues:Iterable[T]

  def tryMergeWithConsistent[V <: TemporalFieldTrait[T]](y: V): Option[V]

  def mergeWithConsistent[V <: TemporalFieldTrait[T]](y: V): V

  /** *
   * creates a new field lineage sket by appending all values in y to the back of this one
   *
   * @param y
   * @return
   */
  def append[V <: TemporalFieldTrait[T]](y: V): V

  def firstTimestamp: LocalDate

  def lastTimestamp: LocalDate

  def getValueLineage: mutable.TreeMap[LocalDate, T]

  def toIntervalRepresentation: mutable.TreeMap[TimeInterval, T]

  //gets the hash values at the specified time-intervals, substituting missing values with the hash-value of ReservedChangeValues.NOT_EXISTANT_ROW
  def valuesAt(timeToExtract: TimeIntervalSequence): Map[TimeInterval, T]

}
