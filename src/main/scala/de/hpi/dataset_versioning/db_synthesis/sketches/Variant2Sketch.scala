package de.hpi.dataset_versioning.db_synthesis.sketches

import java.math.BigInteger
import java.nio.{ByteBuffer, ByteOrder}
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.change.temporal_tables.TimeInterval
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.Variant2Sketch.{byteArraySliceToInt, byteToTimestamp, timestampToByteRepresentation}
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.collection.mutable.HashMap


class Variant2Sketch(data:Array[Byte]) extends FieldLineageSketch {

  assert(data.size % (Variant2Sketch.HASH_VALUE_SIZE_IN_BYTES + 1) ==0)

  private def serialVersionUID = 6529685098267757688L

  override def getBytes = data

  override def hashValueAt(timestamp: LocalDate): Unit = ???

  override def getVariantName: String = Variant2Sketch.getVariantName

  def getIthTimestampIndexInByteArray(ithTimestamp: Int) = ithTimestamp * (Variant2Sketch.HASH_VALUE_SIZE_IN_BYTES + 1)

  //returns the index of the timestamp - not the index of the timestamp in the data array
  def findIndexOfTimestampOrLargestTimestampBefore(ts: LocalDate):Int = {
    val tsAsByte = timestampToByteRepresentation(ts)
    //starting with the first, every 5th byte is a timestamp
    //do binary search:
    var start = 0
    var end = data.size / (Variant2Sketch.HASH_VALUE_SIZE_IN_BYTES + 1)
    //TODO: binary search only if this is a bottleneck: val elem = binaryTimestampSearch(start,end,tsAsByte)
    if(data(start)>tsAsByte){
      -1
    } else {
      var cur = start
      while(cur<end){
        if(data(getIthTimestampIndexInByteArray(cur))==tsAsByte)
          return cur
        else if(data(getIthTimestampIndexInByteArray(cur)) > tsAsByte){
          return cur-1
        }else {
          cur+=1
        }
      }
      end-1
    }
  }

  def numEntries = data.size / (Variant2Sketch.HASH_VALUE_SIZE_IN_BYTES + 1)

  def getHashesInInterval(ti: TimeInterval) :collection.Map[TimeInterval,Int] = {
    var i = findIndexOfTimestampOrLargestTimestampBefore(ti.begin)
    val timeIntervalsToValueMap = HashMap[TimeInterval,Int]()
    if(i== -1){
      //inset row deleted from ti.begin to timestamp(0)
      val rowDeleteHashAsInt = Variant2Sketch.byteArrayToInt(Variant2Sketch.ROWDELETEHASHVALUE)
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

  override def hashValuesAt(timeToExtract: TimeIntervalSequence) = {
    val a = timeToExtract.sortedTimeIntervals.flatMap(ti => {
      getHashesInInterval(ti)
    }).toMap
    a
  }

  def toHashValueLineage = {
    mutable.TreeMap[LocalDate,Int]() ++ (0 until numEntries).map(i => {
      val timestampIndex = i * 5
      (byteToTimestamp(data(timestampIndex)),byteArraySliceToInt(data,timestampIndex+1,timestampIndex+5))
    })
  }

}

object Variant2Sketch {

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
    var hash = if(v == null) "null".hashCode else v.hashCode()
    if(hash==WILDCARD)
      hash = HASH_DJB2(if(v==null) "null" else v.toString)
    if(hash == 0) {
      //bad luck XD
      hash = 1
    }
    intToByteArray(hash)
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

  def fromValueLineage(vl:ValueLineage) = {
    val byteArray = vl.lineage
      .flatMap{case (ts,v) => Seq(timestampToByteRepresentation(ts)) ++ HASH_FUNCTION_STANDARD(v)}
      .toArray
    new Variant2Sketch(byteArray)
  }
}
