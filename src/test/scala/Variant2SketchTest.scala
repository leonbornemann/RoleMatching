import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.change.temporal_tables.TimeInterval
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.Variant2Sketch
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object Variant2SketchTest extends App {

  def toDate(i: Int) = {
   LocalDate.ofEpochDay(IOService.STANDARD_TIME_FRAME_START.toEpochDay + i)
  }

  def toHashAsInt(v: Any) = Variant2Sketch.byteArrayToInt(Variant2Sketch.HASH_FUNCTION_STANDARD(v))

  hashValuesAtIntervalTest
  mergeWithConsistentTest


  def mergeWithConsistentTest = {
    //non wildcard merges:
    //b included in a
    //b after a
    var valuesA = mutable.TreeMap[LocalDate,Any](toDate(3) -> "firstElem",
      toDate(4) -> "secondElem",
      toDate(7) -> ReservedChangeValues.NOT_EXISTANT_ROW,
      toDate(10) -> "thirdElem",
    )
    var valuesB = mutable.TreeMap[LocalDate,Any](toDate(12) -> "thirdElem")
    var a = Variant2Sketch.fromValueLineage(ValueLineage(valuesA))
    var b = Variant2Sketch.fromValueLineage(ValueLineage(valuesB))
    var res = a.mergeWithConsistent(b)
    var resSwapped = b.mergeWithConsistent(a)
    assert(res == resSwapped && res==a)
    //overlap of exactly one day
    valuesA = mutable.TreeMap[LocalDate,Any](toDate(3) -> "firstElem",
      toDate(4) -> ReservedChangeValues.NOT_EXISTANT_COL
    )
    valuesB = mutable.TreeMap[LocalDate,Any](toDate(4) -> "firstElem",
      toDate(5) -> ReservedChangeValues.NOT_EXISTANT_COL)
    var expectedResultValues = mutable.TreeMap[LocalDate,Any](toDate(3) -> "firstElem",
      toDate(5) -> ReservedChangeValues.NOT_EXISTANT_COL)
    a = Variant2Sketch.fromValueLineage(ValueLineage(valuesA))
    b = Variant2Sketch.fromValueLineage(ValueLineage(valuesB))
    res = a.mergeWithConsistent(b)
    resSwapped = b.mergeWithConsistent(a)

  }

  private def hashValuesAtIntervalTest = {
    val values = mutable.TreeMap[LocalDate,Any](toDate(1) -> "firstElem",
      toDate(5) -> "secondElem",
      toDate(15) -> ReservedChangeValues.NOT_EXISTANT_ROW,
      toDate(20) -> "thirdElem",
    )
    val fieldLineage = ValueLineage(values)
    val sketch = Variant2Sketch.fromValueLineage(fieldLineage)
    assert(sketch.numEntries == 4)
    val fieldLineageHashed: mutable.TreeMap[LocalDate, Int] = fieldLineage.lineage.map { case (k, v) => (k, toHashAsInt(v)) }
    assert(sketch.toHashValueLineage == fieldLineageHashed)
    var res = sketch.getHashesInInterval(TimeInterval(toDate(2), Some(toDate(2))))
    var expectedRes = Map(TimeInterval(toDate(2), Some(toDate(2))) -> toHashAsInt("firstElem"))
    assert(res == expectedRes)
    var interval: TimeInterval = TimeInterval(toDate(2), Some(toDate(4)))
    res = sketch.getHashesInInterval(interval)
    expectedRes = Map(interval -> toHashAsInt("firstElem"))
    assert(res == expectedRes)
    interval = TimeInterval(toDate(14), Some(toDate(15)))
    res = sketch.getHashesInInterval(interval)
    expectedRes = Map(TimeInterval(toDate(14), Some(toDate(14))) -> toHashAsInt("secondElem"),
      TimeInterval(toDate(15), Some(toDate(15))) -> toHashAsInt(ReservedChangeValues.NOT_EXISTANT_ROW))
    assert(res == expectedRes)
    interval = TimeInterval(toDate(13), Some(toDate(16)))
    res = sketch.getHashesInInterval(interval)
    expectedRes = Map(TimeInterval(toDate(13), Some(toDate(14))) -> toHashAsInt("secondElem"),
      TimeInterval(toDate(15), Some(toDate(16))) -> toHashAsInt(ReservedChangeValues.NOT_EXISTANT_ROW))
    assert(res == expectedRes)
    interval = TimeInterval(toDate(0), Some(toDate(7)))
    res = sketch.getHashesInInterval(interval)
    expectedRes = Map(TimeInterval(toDate(0), Some(toDate(0))) -> toHashAsInt(ReservedChangeValues.NOT_EXISTANT_ROW),
      TimeInterval(toDate(1), Some(toDate(4))) -> toHashAsInt("firstElem"),
      TimeInterval(toDate(5), Some(toDate(7))) -> toHashAsInt("secondElem"))
    assert(res == expectedRes)
    interval = TimeInterval(toDate(30), Some(toDate(40)))
    res = sketch.getHashesInInterval(interval)
    expectedRes = Map(TimeInterval(toDate(30), Some(toDate(40))) -> toHashAsInt("thirdElem"))
    assert(res == expectedRes)
    //multiple time intervals:
    val is = new TimeIntervalSequence(IndexedSeq(TimeInterval(toDate(0), Some(toDate(7))),
      TimeInterval(toDate(10), Some(toDate(12))),
      TimeInterval(toDate(30), Some(toDate(40)))))
    val resIs = sketch.hashValuesAt(is)
    expectedRes = Map(
      TimeInterval(toDate(0), Some(toDate(0))) -> toHashAsInt(ReservedChangeValues.NOT_EXISTANT_ROW),
      TimeInterval(toDate(1), Some(toDate(5))) -> toHashAsInt("firstElem"),
      TimeInterval(toDate(5), Some(toDate(7))) -> toHashAsInt("secondElem"),
      TimeInterval(toDate(10), Some(toDate(12))) -> toHashAsInt("secondElem"),
      TimeInterval(toDate(30), Some(toDate(40))) -> toHashAsInt("thirdElem"))
  }

}
