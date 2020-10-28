package de.hpi.dataset_versioning.db_synthesis.bottom_up

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.change.temporal_tables.TimeInterval
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.sketches.field.{AbstractTemporalField, TemporalFieldTrait}

import scala.collection.mutable

@SerialVersionUID(3L)
case class ValueLineage(lineage:mutable.TreeMap[LocalDate,Any] = mutable.TreeMap[LocalDate,Any]()) extends AbstractTemporalField[Any] with Serializable{

  private def serialVersionUID = 42L

  def toSerializationHelper = {
    ValueLineageWithHashMap(lineage.toMap)
  }

  def valueAt(ts: LocalDate) = {
    if(lineage.contains(ts))
      lineage(ts)
    else
      lineage.maxBefore(ts)
        .getOrElse(ReservedChangeValues.NOT_EXISTANT_ROW)
  }


  override def toString: String = "[" + lineage.values.mkString("|") + "]"

  override def firstTimestamp: LocalDate = lineage.firstKey

  override def lastTimestamp: LocalDate = lineage.lastKey

  override def getValueLineage: mutable.TreeMap[LocalDate, Any] = lineage

  def isWildcard(value: Any) = ValueLineage.isWildcard(value)

  override def valuesAreCompatible(a: Any, b: Any): Boolean = if(isWildcard(a) || isWildcard(b)) true else a == b

  override def getCompatibleValue(a: Any, b: Any): Any = if(a==b) a else if(isWildcard(a)) b else a

  override def valuesInInterval(ti: TimeInterval): IterableOnce[(TimeInterval, Any)] = {
    var toReturn = toIntervalRepresentation
      .withFilter{case (curTi,v) => !curTi.endOrMax.isBefore(ti.begin) && !curTi.begin.isAfter(ti.endOrMax)}
      .map{case (curTi,v) =>
        val end = Seq(curTi.endOrMax,ti.endOrMax).min
        val begin = Seq(curTi.begin,ti.begin).max
        (TimeInterval(begin,Some(`end`)),v)
      }
    if(ti.begin.isBefore(firstTimestamp))
      toReturn += ((TimeInterval(ti.begin,Some(firstTimestamp)),ReservedChangeValues.NOT_EXISTANT_ROW))
    toReturn
  }

  override def fromValueLineage[V <: TemporalFieldTrait[Any]](lineage: ValueLineage): V = lineage.asInstanceOf[V]

  override def fromTimestampToValue[V <: TemporalFieldTrait[Any]](asTree: mutable.TreeMap[LocalDate, Any]): V = ValueLineage(asTree).asInstanceOf[V]

  override def nonWildCardValues: Iterable[Any] = getValueLineage.values.filter(!isWildcard(_))

  override def isRowDelete(a: Any): Boolean = a==ReservedChangeValues.NOT_EXISTANT_ROW

  override def numValues: Int = lineage.size
}
object ValueLineage{

  def isWildcard(value: Any) = value == ReservedChangeValues.NOT_EXISTANT_DATASET || value == ReservedChangeValues.NOT_EXISTANT_COL
}