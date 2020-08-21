package de.hpi.dataset_versioning.db_synthesis.sketches

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.change.temporal_tables.TimeInterval
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence

trait FieldLineageSketch extends Serializable{

  private def serialVersionUID = 6529685098267757689L

  def getVariantName:String


  def hashValueAt(timestamp:LocalDate)
  def getBytes:Array[Byte]

  //gets the hash values at the specified time-intervals, substituting missing values with the hash-value of ReservedChangeValues.NOT_EXISTANT_ROW
  def hashValuesAt(timeToExtract: TimeIntervalSequence):Map[TimeInterval,Int]

}
