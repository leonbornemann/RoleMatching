package de.hpi.dataset_versioning.db_synthesis.bottom_up

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ReservedChangeValues

import scala.collection.mutable

case class ValueLineage(lineage:mutable.TreeMap[LocalDate,Any] = mutable.TreeMap[LocalDate,Any]()) {

  def toSerializationHelper = {
    ValueLineageWithHashMap(lineage.toMap)
  }


  def valueAt(ts: LocalDate) = {
    if(lineage.contains(ts))
      lineage(ts)
    else
      lineage.maxBefore(ts)
        .getOrElse(ReservedChangeValues.NOT_EXISTANT)
  }


  override def toString: String = "[" + lineage.values.mkString("|") + "]"
}
