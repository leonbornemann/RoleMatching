package de.hpi.tfm.data.tfmp_input.table.nonSketch

import de.hpi.tfm.data.socrata.change.ReservedChangeValues
import de.hpi.tfm.data.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage.WILDCARD_VALUES
import de.hpi.tfm.data.tfmp_input.table.{AbstractTemporalField, TemporalFieldTrait}
import de.hpi.tfm.io.IOService

import java.time.LocalDate
import scala.collection.mutable

@SerialVersionUID(3L)
case class FactLineage(lineage:mutable.TreeMap[LocalDate,Any] = mutable.TreeMap[LocalDate,Any]()) extends AbstractTemporalField[Any] with Serializable{

  def projectToTimeRange(timeRangeStart: LocalDate, timeRangeEnd: LocalDate) = {
    val prevStart = lineage.firstKey
    val afterStart = lineage.filter { case (k, v) => !k.isBefore(timeRangeStart) && !k.isAfter(timeRangeEnd) }
    if(afterStart.isEmpty){
      val last = lineage.maxBefore(timeRangeStart).get
      FactLineage(mutable.TreeMap((timeRangeStart,last._2)))
    } else{
      if(afterStart.firstKey!=timeRangeStart){
        if(!lineage.maxBefore(afterStart.firstKey).isDefined){
          println("what?")
          println(this)
          println(this.lineage)
          println(timeRangeStart)
          println(timeRangeEnd)
        }
        val before = lineage.maxBefore(afterStart.firstKey).get
        assert(before._1.isBefore(timeRangeStart))
        afterStart.put(timeRangeStart,before._2)
      }
      assert(afterStart.firstKey==timeRangeStart)
      assert(prevStart == lineage.firstKey)
      FactLineage(afterStart)
    }
  }

  def keepOnlyStandardTimeRange = FactLineage(lineage.filter(!_._1.isAfter(IOService.STANDARD_TIME_FRAME_END)))


  private def serialVersionUID = 42L

  def toSerializationHelper = {
    FactLineageWithHashMap(lineage.toMap)
  }

  override def valueAt(ts: LocalDate) = {
    if(lineage.contains(ts))
      lineage(ts)
    else {
      val res = lineage.maxBefore(ts)
      if(res.isDefined) {
        res.get._2
      } else {
        ReservedChangeValues.NOT_EXISTANT_ROW
      }
    }
  }

  override def toString: String = "[" + lineage.values.mkString("|") + "]"

  override def firstTimestamp: LocalDate = lineage.firstKey

  override def lastTimestamp: LocalDate = lineage.lastKey

  override def getValueLineage: mutable.TreeMap[LocalDate, Any] = lineage

  def isWildcard(value: Any) = FactLineage.isWildcard(value)

  override def valuesAreCompatible(a: Any, b: Any): Boolean = if(isWildcard(a) || isWildcard(b)) true else a == b

  override def getCompatibleValue(a: Any, b: Any): Any = if(a==b) a else if(isWildcard(a)) b else a

  def valuesInInterval(ti: TimeInterval): IterableOnce[(TimeInterval, Any)] = {
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

  override def fromValueLineage[V <: TemporalFieldTrait[Any]](lineage: FactLineage): V = lineage.asInstanceOf[V]

  override def fromTimestampToValue[V <: TemporalFieldTrait[Any]](asTree: mutable.TreeMap[LocalDate, Any]): V = FactLineage(asTree).asInstanceOf[V]

  override def nonWildCardValues: Iterable[Any] = getValueLineage.values.filter(!isWildcard(_))

  override def numValues: Int = lineage.size

  override def allTimestamps: Iterable[LocalDate] = lineage.keySet

  override def WILDCARDVALUES: Set[Any] = WILDCARD_VALUES
}
object FactLineage{

  def WILDCARD_VALUES:Set[Any] = Set(ReservedChangeValues.NOT_EXISTANT_COL,ReservedChangeValues.NOT_EXISTANT_COL,ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_CELL,ReservedChangeValues.NOT_KNOWN_DUE_TO_TIMESTAMP_RESOLUTION)

  def tryMergeAll(toMerge: IndexedSeq[FactLineage]) = {
    var res = Option(toMerge.head)
    (1 until toMerge.size).foreach(i => {
      if(res.isDefined)
        res = res.get.tryMergeWithConsistent(toMerge(i))
    })
    res
  }


  def fromSerializationHelper(valueLineageWithHashMap: FactLineageWithHashMap) = FactLineage(mutable.TreeMap[LocalDate,Any]() ++ valueLineageWithHashMap.lineage)

  def isWildcard(value: Any) = WILDCARD_VALUES.contains(value)

}