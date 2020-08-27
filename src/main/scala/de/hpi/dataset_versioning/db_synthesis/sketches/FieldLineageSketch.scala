package de.hpi.dataset_versioning.db_synthesis.sketches

import java.time.LocalDate
import java.time.chrono.ChronoLocalDate

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.change.temporal_tables.TimeInterval
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence

import scala.collection.mutable

trait FieldLineageSketch extends Serializable{
  def changeCount: Int

  def firstTimestamp: LocalDate


  /***
   * creates a new field lineage sket by appending all values in y to the back of this one
   *
   * @param y
   * @return
   */
  def append(y: FieldLineageSketch): FieldLineageSketch


  def toHashValueLineage:mutable.TreeMap[LocalDate,Int]
  def toIntervalRepresentation:mutable.TreeMap[TimeInterval,Int]

  def mergeWithConsistent(other: FieldLineageSketch): FieldLineageSketch

  def lastTimestamp: LocalDate


  private def serialVersionUID = 6529685098267757689L

  def getVariantName:String


  def hashValueAt(timestamp:LocalDate)
  def getBytes:Array[Byte]

  //gets the hash values at the specified time-intervals, substituting missing values with the hash-value of ReservedChangeValues.NOT_EXISTANT_ROW
  def hashValuesAt(timeToExtract: TimeIntervalSequence):Map[TimeInterval,Int]

}
