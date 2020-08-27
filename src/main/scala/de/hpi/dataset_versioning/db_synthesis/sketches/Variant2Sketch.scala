package de.hpi.dataset_versioning.db_synthesis.sketches

import java.math.BigInteger
import java.nio.{ByteBuffer, ByteOrder}
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.change.temporal_tables.TimeInterval
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.Variant2Sketch.{WILDCARD, byteArraySliceToInt, byteToTimestamp, timestampToByteRepresentation}
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.collection.mutable.HashMap


class Variant2Sketch(data:Array[Byte]) extends FieldLineageSketch {

  assert(byteToTimestamp(0)==IOService.STANDARD_TIME_FRAME_START) //we need this for all lineages because otherwise we don't know if it is wildcard or not!

  override def hashCode(): Int = getBytes.toIndexedSeq.hashCode()

  override def equals(o: Any): Boolean = {
    if(o.isInstanceOf[Variant2Sketch])
      getBytes.toIndexedSeq == o.asInstanceOf[Variant2Sketch].getBytes.toIndexedSeq
    else
      false
  }

  def toIntervalRepresentation:mutable.TreeMap[TimeInterval,Int] = {
    val asLineage = toHashValueLineage.toIndexedSeq
    mutable.TreeMap[TimeInterval,Int]() ++ (0 until asLineage.size).map( i=> {
      val (ts,value) = asLineage(i)
      if(i==asLineage.size-1)
        (TimeInterval(ts,None),value)
      else
        (TimeInterval(ts,Some(asLineage(i+1)._1)),value)
    })
  }


  assert(data.size % (Variant2Sketch.HASH_VALUE_SIZE_IN_BYTES + 1) ==0)

  private def serialVersionUID = 6529685098267757688L

  override def getBytes = data

  override def hashValueAt(timestamp: LocalDate): Unit = ???

  override def getVariantName: String = Variant2Sketch.getVariantName

  def getIthTimestampIndexInByteArray(ithTimestamp: Int) = ithTimestamp * (Variant2Sketch.HASH_VALUE_SIZE_IN_BYTES + 1)

  //returns the index of the timestamp - not the index of the timestamp in the data array
  private def findIndexOfTimestampOrLargestTimestampBefore(ts: LocalDate):Int = {
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

  override def lastTimestamp: LocalDate = byteToTimestamp(data(getIthTimestampIndexInByteArray(numEntries-1)))

  def hashValuesAreCompatible(value1: Int, value2: Int): Boolean = {
    value1 == WILDCARD || value2 == WILDCARD || value1==value2
  }

  def getCompatibleHashValue(a: Int, b: Int): Int = {
    if(a==b) a else if(a==WILDCARD) b else a
  }

  def getOverlapInterval(a: (TimeInterval, Int), b: (TimeInterval, Int)): (TimeInterval, Int) = {
    assert(a._1.begin==b._1.begin)
    if(!hashValuesAreCompatible(a._2,b._2)){
      println()
    }
    assert(hashValuesAreCompatible(a._2,b._2))
    val earliestEnd = Seq(a._1.endOrMax,b._1.endOrMax).minBy(_.toEpochDay)
    val endTime = if(earliestEnd==LocalDate.MAX) None else Some(earliestEnd)
    (TimeInterval(a._1.begin,endTime),getCompatibleHashValue(a._2,b._2))
  }

  override def mergeWithConsistent(other: FieldLineageSketch): FieldLineageSketch = {
    val myLineage = this.toIntervalRepresentation.toBuffer
    val otherLineage = other.toIntervalRepresentation.toBuffer
    if(myLineage.isEmpty){
      assert(otherLineage.isEmpty)
      Variant2Sketch.fromValueLineage(ValueLineage())
    } else if(otherLineage.isEmpty){
      assert(myLineage.isEmpty)
      Variant2Sketch.fromValueLineage(ValueLineage())
    }
    val newLineage = mutable.ArrayBuffer[(TimeInterval,Int)]()
    var myIndex = 0
    var otherIndex = 0
    while(myIndex < myLineage.size || otherIndex < otherLineage.size) {
      assert(myIndex < myLineage.size && otherIndex < otherLineage.size)
      val (myInterval,myValue) = myLineage(myIndex)
      val (otherInterval,otherValue) = otherLineage(otherIndex)
      assert(myInterval.begin == otherInterval.begin)
      var toAppend:(TimeInterval,Int) = null
      if(myInterval==otherInterval){
        if(!hashValuesAreCompatible(myValue,otherValue))
          println()
        assert(hashValuesAreCompatible(myValue,otherValue))
        toAppend = (myInterval,getCompatibleHashValue(myValue,otherValue))
        myIndex+=1
        otherIndex+=1
      } else if(myInterval<otherInterval){
        toAppend = getOverlapInterval(myLineage(myIndex),otherLineage(otherIndex))
        //replace old interval with newer interval with begin set to myInterval.end+1
        otherLineage(otherIndex) = (TimeInterval(myInterval.end.get,otherInterval.`end`),otherValue)
        myIndex+=1
      } else{
        assert(otherInterval<myInterval)
        toAppend = getOverlapInterval(myLineage(myIndex),otherLineage(otherIndex))
        myLineage(myIndex) = (TimeInterval(otherInterval.end.get,myInterval.`end`),myValue)
        otherIndex+=1
      }
      if(!newLineage.isEmpty && newLineage.last._2==toAppend._2){
        //we replace the old interval by a longer one
        newLineage(newLineage.size-1) = (TimeInterval(newLineage.last._1.begin,toAppend._1.`end`),toAppend._2)
      } else{
        //we simply append the new interval:
        newLineage += toAppend
      }
    }
    val asTree = mutable.TreeMap[LocalDate,Int]() ++ newLineage.map(t => (t._1.begin,t._2))
    Variant2Sketch.fromTimestampToHash(asTree)
  }

  /** *
   * creates a new field lineage sket by appending all values in y to the back of this one
   *
   * @param y
   * @return
   */
  override def append(y: FieldLineageSketch): FieldLineageSketch = {
    assert(lastTimestamp.isBefore(y.firstTimestamp))
    new Variant2Sketch(getBytes ++ y.getBytes)
  }

  override def firstTimestamp: LocalDate = byteToTimestamp(data(0))

  override def changeCount: Int = numEntries
}

object Variant2Sketch {
  def fromTimestampToHash(asTree: mutable.TreeMap[LocalDate, Int]) = {
    val bytes = asTree.flatMap{case (k,v) => Seq(timestampToByteRepresentation(k)) ++ intToByteArray(v)}.toArray
    new Variant2Sketch(bytes)
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
    if(v==ReservedChangeValues.NOT_EXISTANT_COL || v == ReservedChangeValues.NOT_EXISTANT_DATASET)
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

  def fromValueLineage(vl:ValueLineage) = {
    val byteArray = vl.lineage
      .flatMap{case (ts,v) => Seq(timestampToByteRepresentation(ts)) ++ HASH_FUNCTION_STANDARD(v)}
      .toArray
    new Variant2Sketch(byteArray)
  }
}
