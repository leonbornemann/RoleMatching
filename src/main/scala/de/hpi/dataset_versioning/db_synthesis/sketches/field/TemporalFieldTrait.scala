package de.hpi.dataset_versioning.db_synthesis.sketches.field

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TimeInterval
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence
import de.hpi.dataset_versioning.db_synthesis.baseline.config.FieldChangeCounter

import scala.collection.mutable

trait TemporalFieldTrait[T] {
  def isRowDelete(a: T) :Boolean

  def isWildcard(a: T) :Boolean


  def countChanges(viewInsertTime:LocalDate,changeCounter:FieldChangeCounter):Int

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
