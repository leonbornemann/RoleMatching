package de.hpi.tfm.data.tfmp_input.table.sketch

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.change.ReservedChangeValues
import de.hpi.tfm.data.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.data.tfmp_input.table.sketch.FactLineageSketch.{WILDCARD, byteArraySliceToInt, byteToTimestamp}
import de.hpi.tfm.io.IOService

import java.nio.{ByteBuffer, ByteOrder}
import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.HashMap

@SerialVersionUID(3L)
class FactLineageSketch(data:Array[Byte]) extends FieldLineageSketch with StrictLogging{

  override def toString: String = FactLineage(mutable.TreeMap[LocalDate,Any]() ++ getValueLineage).toString

  assert(byteToTimestamp(0)==IOService.STANDARD_TIME_FRAME_START) //we need this for all lineages because otherwise we don't know if it is wildcard or not!

  override def hashCode(): Int = getBytes.toIndexedSeq.hashCode()

  override def equals(o: Any): Boolean = {
    if(o.isInstanceOf[FactLineageSketch])
      getBytes.toIndexedSeq == o.asInstanceOf[FactLineageSketch].getBytes.toIndexedSeq
    else
      false
  }

  assert(data.size % (FactLineageSketch.HASH_VALUE_SIZE_IN_BYTES + 1) ==0)

  override def getBytes = data

  override def getVariantName: String = FactLineageSketch.getVariantName

  def getIthTimestampIndexInByteArray(ithTimestamp: Int) = ithTimestamp * (FactLineageSketch.HASH_VALUE_SIZE_IN_BYTES + 1)

  //returns the index of the timestamp - not the index of the timestamp in the data array
  private def findIndexOfTimestampOrLargestTimestampBeforeOrEqual(ts: LocalDate):Int = {
    //starting with the first, every 5th byte is a timestamp
    //do binary search:
    var start = 0
    var end = data.size / (FactLineageSketch.HASH_VALUE_SIZE_IN_BYTES + 1)
    //TODO: binary search only if this is a bottleneck: val elem = binaryTimestampSearch(start,end,tsAsByte)
    if(byteToTimestamp(data(start)).isAfter(ts)){
      -1
    } else {
      var cur = start
      while(cur<end){
        if(byteToTimestamp(data(getIthTimestampIndexInByteArray(cur)))==ts)
          return cur
        else if(byteToTimestamp(data(getIthTimestampIndexInByteArray(cur))).isAfter(ts)){
          return cur-1
        }else {
          cur+=1
        }
      }
      end-1
    }
  }

  def numEntries = data.size / (FactLineageSketch.HASH_VALUE_SIZE_IN_BYTES + 1)

  def valuesInInterval(ti: TimeInterval) :collection.Map[TimeInterval,Int] = {
    var i = findIndexOfTimestampOrLargestTimestampBeforeOrEqual(ti.begin)
    val timeIntervalsToValueMap = HashMap[TimeInterval,Int]()
    if(i== -1){
      //actually, this should never happen again
      logger.warn("Weird behavior: we should never get here again with the new change format")
      //inset row deleted from ti.begin to timestamp(0)
      val rowDeleteHashAsInt = FactLineageSketch.byteArrayToInt(FactLineageSketch.ROWDELETEHASHVALUE)
      timeIntervalsToValueMap.put(new TimeInterval(ti.begin,Some(byteToTimestamp(data(0)).minusDays(1))),rowDeleteHashAsInt)
      i = 0
    }
    var curTs = byteToTimestamp(data(getIthTimestampIndexInByteArray(i)))
    val intervalEnd = ti.endOrMax
    while(!curTs.isAfter(intervalEnd) && i<numEntries){
      val nextTsByteIndex = getIthTimestampIndexInByteArray(i + 1)
      val curIntervalStart = Seq(curTs,ti.begin).maxBy(_.toEpochDay) //if interval start is after curTs we start with interval start
      if(i==numEntries-1){
        timeIntervalsToValueMap.put(TimeInterval(curIntervalStart,Some(intervalEnd)),byteArraySliceToInt(data,nextTsByteIndex-4,nextTsByteIndex))
        i+=1
      } else {
        val nextTs = byteToTimestamp(data(nextTsByteIndex))
        if (nextTs.isAfter(intervalEnd))
          timeIntervalsToValueMap.put(TimeInterval(curIntervalStart, Some(intervalEnd)), byteArraySliceToInt(data, nextTsByteIndex - 4, nextTsByteIndex))
        else {
          timeIntervalsToValueMap.put(TimeInterval(curIntervalStart, Some(nextTs.minusDays(1))), byteArraySliceToInt(data, nextTsByteIndex - 4, nextTsByteIndex))
        }
        curTs = nextTs
        i += 1
      }
    }
    timeIntervalsToValueMap
  }

  def getValueLineage = {
    mutable.TreeMap[LocalDate,Int]() ++ (0 until numEntries).map(i => {
      val timestampIndex = i * 5
      (byteToTimestamp(data(timestampIndex)),byteArraySliceToInt(data,timestampIndex+1,timestampIndex+5))
    })
  }

  override def lastTimestamp: LocalDate = byteToTimestamp(data(getIthTimestampIndexInByteArray(numEntries-1)))

  def valuesAreCompatible(value1: Int, value2: Int): Boolean = {
    value1 == WILDCARD || value2 == WILDCARD || value1==value2
  }

  def getCompatibleValue(a: Int, b: Int): Int = {
    if(a==b) a else if(a==WILDCARD) b else a
  }

  override def firstTimestamp: LocalDate = byteToTimestamp(data(0))

  //override def valuesAt(timeToExtract: TimeIntervalSequence): Map[TimeInterval, Int] = hashValuesAt(timeToExtract)

  //override def hashValuesAt(timeToExtract: TimeIntervalSequence): Map[TimeInterval, Int] = ???

  override def fromValueLineage[V <: TemporalFieldTrait[Int]](lineage: FactLineage): V = FactLineageSketch.fromValueLineage(lineage).asInstanceOf[V]

  //override def fromTimestampToValue[V <: TemporalFieldTrait[Int]](asTree: mutable.TreeMap[LocalDate, Any]): V = ???
  override def fromTimestampToValue[V <: TemporalFieldTrait[Int]](asTree: mutable.TreeMap[LocalDate, Int]): V = FactLineageSketch.fromTimestampToHash(asTree).asInstanceOf[V]

  def getIthValueAsInt(i: Int): Int = {
    byteArraySliceToInt(data,i*5+1,i*5+5)
  }

  override def valueAt(ts: LocalDate): Int = {
    if(ts.isBefore(firstTimestamp))
      FactLineageSketch.byteArrayToInt(FactLineageSketch.ROWDELETEHASHVALUE)
    else{
      val timestampIndex = findIndexOfTimestampOrLargestTimestampBeforeOrEqual(ts)
      if(timestampIndex== -1){
        println("OUCH")
        println(ts)
        println("------------")
        getValueLineage.toIndexedSeq.sortBy(_._1.toEpochDay).foreach(println(_))
      }
      assert(timestampIndex!= -1)
      getIthValueAsInt(timestampIndex)
    }
  }

  override def nonWildCardValues: Iterable[Int] = getValueLineage.values.filter(_ != WILDCARD)

  override def isWildcard(a: Int): Boolean = a==WILDCARD

  override def numValues: Int = numEntries

  override def WILDCARDVALUES: Set[Int] = Set(WILDCARD)
}

object FactLineageSketch {
  def fromTimestampToHash(asTree: mutable.TreeMap[LocalDate, Int]) = {
    if(asTree.isEmpty)
      throw new AssertionError("not allowed to create empty lineages")
    val bytes = asTree.flatMap{case (k,v) => Seq(timestampToByteRepresentation(k)) ++ intToByteArray(v)}.toArray
    new FactLineageSketch(bytes)
  }

  def HASH_DJB2(v:String) = {
    //source: http://www.cse.yorku.ca/~oz/hash.html
    var hash = 5381
    v.foreach(c => {
      hash = ((hash << 5) + hash) + c
    })
    hash
  }

  //byteArraySliceToInt()

//  def intToByteArray(hash: Int) = {
//    import java.nio.ByteBuffer
//    import java.nio.ByteOrder
//    val bb = ByteBuffer.wrap(hash).order(ByteOrder.LITTLE_ENDIAN)
//    val byteArray = Array[Byte]()
//    //    BigInteger.valueOf(hash).toByteArray
//  }

  def byteArraySliceToInt(array:Array[Byte], start:Int, end:Int) = {
    assert(end == start+4)
    val sliced = array.slice(start,end)
    byteArrayToInt(sliced)
  }

  def byteArrayToInt(array:Array[Byte]) = {
    assert(array.size==4)
    ByteBuffer.wrap(array).order(ByteOrder.LITTLE_ENDIAN).getInt()
  }

  def intToByteArray(hash: Int) = {
    val a = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(hash).array()
    a
  }

  def HASH_FUNCTION_STANDARD(v:Any):Array[Byte] = {
    if(v==ReservedChangeValues.NOT_EXISTANT_COL || v == ReservedChangeValues.NOT_EXISTANT_DATASET || v == ReservedChangeValues.NOT_EXISTANT_ROW)
      intToByteArray(WILDCARD)
    else {
      var hash = if (v == null) "null".hashCode else v.hashCode()
      if (hash == WILDCARD)
        hash = HASH_DJB2(if (v == null) "null" else v.toString)
      if (hash == 0) {
        //bad luck XD
        hash = 1
      }
      intToByteArray(hash)
    }
  }

  def getVariantName: String = "timestampValuePairs"

  val HASH_VALUE_SIZE_IN_BYTES = 4
  //reserved Hash value:
  val WILDCARD = 0
  val ROWDELETEHASHVALUE = HASH_FUNCTION_STANDARD(ReservedChangeValues.NOT_EXISTANT_ROW)

  val timestampToByteRepresentation = {
    (IOService.STANDARD_TIME_FRAME_START.toEpochDay to IOService.STANDARD_TIME_FRAME_END.toEpochDay)
      .zipWithIndex
      .map{case (ts,i) => (LocalDate.ofEpochDay(ts),i.toByte)}
      .toMap
  }

  val byteToTimestamp = timestampToByteRepresentation.map(t => (t._2,t._1))

  def fromValueLineage(vl:FactLineage) = {
    if(vl.lineage.isEmpty){
      throw new AssertionError("not allowed to create empty lineages")
    }
    val byteArray = vl.lineage
      .flatMap{case (ts,v) => Seq(timestampToByteRepresentation(ts)) ++ HASH_FUNCTION_STANDARD(v)}
      .toArray
    new FactLineageSketch(byteArray)
  }
}
