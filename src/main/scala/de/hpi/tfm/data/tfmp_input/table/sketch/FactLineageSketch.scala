package de.hpi.tfm.data.tfmp_input.table.sketch

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.change.ReservedChangeValues
import de.hpi.tfm.data.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.data.tfmp_input.table.sketch.FactLineageSketch.{WILDCARD}
import de.hpi.tfm.io.IOService

import java.nio.{ByteBuffer, ByteOrder}
import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.HashMap

@SerialVersionUID(3L)
class FactLineageSketch(val factLineage: FactLineage) extends FieldLineageSketch with StrictLogging{

  assert(factLineage.lineage.forall(_._2.isInstanceOf[Int]))

  override def toString: String = factLineage.toString

  override def hashCode(): Int = factLineage.lineage.hashCode()

  override def equals(o: Any): Boolean = {
    if(o.isInstanceOf[FactLineageSketch])
      factLineage == o.asInstanceOf[FactLineageSketch].factLineage
    else
      false
  }

  override def getVariantName: String = FactLineageSketch.getVariantName

  def numEntries = factLineage.lineage.size

  def getValueLineage = {
    factLineage.lineage.map(t => (t._1,t._2.asInstanceOf[Int]))
  }

  override def lastTimestamp: LocalDate = factLineage.lastTimestamp

  def valuesAreCompatible(value1: Int, value2: Int): Boolean = {
    value1 == WILDCARD || value2 == WILDCARD || value1==value2
  }

  def getCompatibleValue(a: Int, b: Int): Int = {
    if(a==b) a else if(a==WILDCARD) b else a
  }

  override def firstTimestamp: LocalDate = factLineage.firstTimestamp

  override def fromValueLineage[V <: TemporalFieldTrait[Int]](lineage: FactLineage): V = FactLineageSketch.fromValueLineage(lineage).asInstanceOf[V]

  override def fromTimestampToValue[V <: TemporalFieldTrait[Int]](asTree: mutable.TreeMap[LocalDate, Int]): V = FactLineageSketch.fromTimestampToHash(asTree).asInstanceOf[V]

  override def valueAt(ts: LocalDate): Int = {
    factLineage.valueAt(ts).asInstanceOf[Int]
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
    val fl = asTree.map(t => (t._1,t._2.asInstanceOf[Any]))
    new FactLineageSketch(FactLineage(fl))
  }

  def HASH_DJB2(v:String) = {
    //source: http://www.cse.yorku.ca/~oz/hash.html
    var hash = 5381
    v.foreach(c => {
      hash = ((hash << 5) + hash) + c
    })
    hash
  }

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

  def HASH_FUNCTION_STANDARD(v:Any):Int = {
    if(FactLineage.isWildcard(v))
      WILDCARD
    else {
      var hash = if (v == null) "null".hashCode else v.hashCode()
      if (hash == WILDCARD)
        hash = HASH_DJB2(if (v == null) "null" else v.toString)
      if (hash == 0) {
        //bad luck XD
        hash = 1
      }
      hash
    }
  }

  def getVariantName: String = "timestampValuePairs"

  //reserved Hash value:
  val WILDCARD = 0

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
    val tsToHash = vl.lineage
      .map{case (ts,v) => (ts,HASH_FUNCTION_STANDARD(v))}
    fromTimestampToHash(tsToHash)
  }
}
