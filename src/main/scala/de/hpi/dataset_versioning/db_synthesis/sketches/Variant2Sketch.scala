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

  def toIntervalRepresentation = {
    val asLineage = toHashValueLineage.toIndexedSeq
    (0 until asLineage.size).map( i=> {
      val (ts,value) = asLineage(i)
      if(i==asLineage.size-1)
        (TimeInterval(ts,None),value)
      else
        (TimeInterval(ts,Some(asLineage(i+1)._1)),value)
    }).toMap
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

  override def mergeWithConsistent(other: FieldLineageSketch): FieldLineageSketch = {
    throw new AssertionError("TODO: this method needs to be completely redone!")
    val myIterator = this.toIntervalRepresentation.iterator
    val otherIterator = other.toIntervalRepresentation.iterator
    val newLineage = mutable.TreeMap[TimeInterval,Int]()
    var myHead = myIterator.nextOption()
    var otherHead = otherIterator.nextOption()
    while(myHead.isDefined || otherHead.isDefined){
      if(!myHead.isDefined){
        newLineage += otherHead.get
        otherHead = otherIterator.nextOption()
      } else if(!otherHead.isDefined){
        newLineage += myHead.get
        myHead = myIterator.nextOption()
      } else {
        val myBegin = myHead.get._1.begin
        val myEnd = myHead.get._1.endOrMax
        val otherBegin = otherHead.get._1.begin
        val otherEnd = otherHead.get._1.endOrMax
        if(myBegin.isAfter(otherEnd)){
          newLineage += otherHead.get
          otherHead = otherIterator.nextOption()
        } else if(otherBegin.isAfter(myEnd)){
          newLineage += myHead.get
          myHead = myIterator.nextOption()
        } else{
          // we have an overlap like this:
          //Interval1 ----------
          //Interval2       ----------------
          //or this:
          //       -------------
          //---------------------------
          assert(hashValuesAreCompatible(myHead.get._2,otherHead.get._2)) //if this does not hold we were not compatible
          val earlierInterval = if(myBegin.isBefore(otherBegin)) myHead.get else otherHead.get
          val laterInterval = if(earlierInterval == myHead.get) otherHead.get else myHead.get
          //TODO: only if we have wildcard in one of these intervals we should do this
          if(earlierInterval._2 == laterInterval._2 ||
            (laterInterval._2==WILDCARD && !laterInterval._1.endOrMax.isAfter(earlierInterval._1.endOrMax)) ){
            //Easy: Time Interval till latest end
            val latestEnd = Seq(earlierInterval._1.endOrMax,laterInterval._1.endOrMax).maxBy(_.toEpochDay)
            val endTs = if(latestEnd==LocalDate.MAX) None else Some(latestEnd)
            val toAppend = (TimeInterval(earlierInterval._1.begin,endTs),earlierInterval._2)
            newLineage += toAppend
          } else if(laterInterval._2==WILDCARD){
            assert(laterInterval._1.endOrMax.isAfter(earlierInterval._1.endOrMax))
            var toAppend = (TimeInterval(earlierInterval._1.begin,earlierInterval._1.`end`),earlierInterval._2)
            newLineage += toAppend
            //we need to preserve the wildcard after
            toAppend = (TimeInterval(earlierInterval._1.`end`.get.plusDays(1),laterInterval._1.`end`),laterInterval._2)
            newLineage += toAppend
          } else if(earlierInterval._2==WILDCARD && earlierInterval._1.begin == laterInterval._1.begin){
            //easy: we can append the later interval:
            newLineage += laterInterval
          } else{
            assert(earlierInterval._2==WILDCARD && laterInterval._1.begin.isAfter(earlierInterval._1.begin))
            //WILDCARD at the beginning:
            var toAppend = (TimeInterval(earlierInterval._1.begin,Some(laterInterval._1.begin.minusDays(1))),WILDCARD)
            newLineage += toAppend
            //The part afterwards:
            //TODO: what about wildcard and after?
            toAppend = (TimeInterval(laterInterval._1.begin,Some(laterInterval._1.begin.minusDays(1))),WILDCARD)
          }
          if(earlierInterval._1.begin!=laterInterval._1.begin){
            val toAppend = (TimeInterval(earlierInterval._1.begin,Some(laterInterval._1.begin.minusDays(1))),earlierInterval._2)
            newLineage += toAppend
          }
          //now the overlapping part
          val earliestEnd = Seq(earlierInterval._1.endOrMax,laterInterval._1.endOrMax).minBy(_.toEpochDay)
          val valueInOverlap = (if(earlierInterval._2==laterInterval._2) earlierInterval._2
            else if(earlierInterval._2==WILDCARD) laterInterval._2
            else earlierInterval._2) //take the non-wildcard value
          val overlapTimestampEnd = if(earliestEnd==LocalDate.MAX) None else Some(earliestEnd)
          var toAppend = (TimeInterval(laterInterval._1.begin,overlapTimestampEnd),valueInOverlap)
          newLineage += toAppend
          //now the part of the later one
          val intervalContinuingAfterOverlap = if(earlierInterval._1.endOrMax.isBefore(earliestEnd)) earlierInterval else laterInterval
          if(intervalContinuingAfterOverlap._1.endOrMax.isBefore(earliestEnd)){
            assert(earliestEnd!=LocalDate.MAX)
            toAppend = (TimeInterval(earliestEnd.plusDays(1),intervalContinuingAfterOverlap._1.`end`),intervalContinuingAfterOverlap._2)
            newLineage += toAppend
          }
          myHead = myIterator.nextOption()
          otherHead = otherIterator.nextOption()
        }
      }
    }
    Variant2Sketch.fromValueLineage(ValueLineage(newLineage.map(t => (t._1.begin,t._2))))
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
