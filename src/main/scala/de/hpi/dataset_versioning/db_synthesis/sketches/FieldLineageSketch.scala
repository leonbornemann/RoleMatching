package de.hpi.dataset_versioning.db_synthesis.sketches

import java.time.LocalDate
import java.time.chrono.ChronoLocalDate

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.change.temporal_tables.TimeInterval
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence

import scala.collection.mutable

trait FieldLineageSketch extends AbstractTemporalField[Int] with Serializable{
  def valueAt(ts: LocalDate): Int


  def getVariantName:String

  def getBytes:Array[Byte]

}
