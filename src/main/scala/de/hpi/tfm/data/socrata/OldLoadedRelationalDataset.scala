package de.hpi.tfm.data.socrata

import com.google.gson._
import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.metadata.custom.joinability.`export`.Column
import de.hpi.tfm.data.socrata.parser.exceptions.SchemaMismatchException
import de.hpi.tfm.io.IOService
import de.hpi.tfm.util.TableFormatter

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class OldLoadedRelationalDataset(val id:String, val version:LocalDate, val rows:ArrayBuffer[scala.collection.IndexedSeq[JsonElement]]=ArrayBuffer()) extends StrictLogging{

  def isProjectionOf(other: OldLoadedRelationalDataset, columnMapping: mutable.HashMap[String, String]): Boolean = {
    if(other.ncols < ncols || columnMapping.size!=ncols)
      false
    else{
      val colRenames = columnMapping.values
        .toIndexedSeq
        .map(s => (s,s))
      val otherProjected = other.getProjection(other.id + "_projection",colRenames)
      isEqualTo(otherProjected,columnMapping)
    }
  }

  def getRowMultiSet = rows.groupBy(identity)
    .mapValues(_.size)

  def isEqualTo(other: OldLoadedRelationalDataset, columnMapping: mutable.HashMap[String, String]): Boolean = {
    //we need to compare all tuples in the order specified by the mapping
    if(other.ncols != ncols || columnMapping.size!=ncols)
      false
    else {
      val tuplesToMatch = mutable.HashMap() ++ other.getRowMultiSet
      var allTuplesFound = true
      val it = rows.iterator
      val otherColNameToIndex = other.colNames.zipWithIndex.toMap
      while (it.hasNext && allTuplesFound) {
        val curRow = it.next()
        val curRowInOtherOrder = mutable.IndexedSeq.fill[JsonElement](curRow.size)(JsonNull.INSTANCE)
        (0 until curRow.size).foreach(myColIndex => {
          val myColName = colNames(myColIndex)
          val otherColIndex = otherColNameToIndex(columnMapping(myColName))
          curRowInOtherOrder(otherColIndex) = curRow(myColIndex)
        })
        val countInOtherDS = tuplesToMatch.getOrElse(curRowInOtherOrder, 0)
        if (countInOtherDS == 0)
          allTuplesFound = false
        else if(countInOtherDS > 1)
          tuplesToMatch(curRowInOtherOrder) = countInOtherDS - 1
        else
          tuplesToMatch.remove(curRowInOtherOrder) //we have matched all tuple duplicates
      }
      allTuplesFound && tuplesToMatch.isEmpty
    }
  }

  def toMultiset[A](list: collection.Seq[A]):Map[A,Int] = list.groupBy(identity).map{case (a,b) => (a,b.size)}

  def getTupleSpecificHash: Int = {
    val tupleMultiset = toMultiset(rows
      .map(tuple => toMultiset(tuple.map(OldLoadedRelationalDataset.getCellValueAsString(_)))))
      /*.sorted(new Ordering[Seq[String]] {
      override def compare(x: Seq[String], y: Seq[String]): Int = {
        val it = x.zip(y).iterator
        var res = 0
        while(it.hasNext && res==0){
          val (s1,s2) = it.next()
          res = s1.compareTo(s2)
        }
        res
      }
    })*/
    tupleMultiset.hashCode()
  }

  def constructRow(r1: collection.IndexedSeq[JsonElement], ds1: OldLoadedRelationalDataset, r2: collection.IndexedSeq[JsonElement], ds2: OldLoadedRelationalDataset, newColumnOrder: collection.IndexedSeq[(String, Int, OldLoadedRelationalDataset)]): collection.IndexedSeq[JsonElement] = {
    val newRow = newColumnOrder.map{case (_,i,sourceDS) =>{
      if(sourceDS==ds1)
        r1(i)
      else if(sourceDS==ds2)
        r2(i)
      else
        throw new AssertionError("weird")
    }}
    newRow
  }

  /***
   * the Join that keeps both join columns
   * @param other
   * @param myJoinColIndex
   * @param otherJoinColIndex
   * @return
   */
  def join(other: OldLoadedRelationalDataset, myJoinColIndex: Short, otherJoinColIndex: Short):OldLoadedRelationalDataset = {
    val myJoinCol = colNames(myJoinColIndex)
    val otherJoinCol = other.colNames(otherJoinColIndex)
    val joinDataset = new OldLoadedRelationalDataset(id + s"_joinedOn($myJoinCol,$otherJoinCol)_with_" +other.id ,version)
    val newColumnOrder = (colNames.zipWithIndex.map{case (a,b) => (a,b,this)}
      ++ other.colNames.zipWithIndex.map{case (a,b) => (a,b,other)})
        .sortBy(t => t._3.id + "." + t._1)
    joinDataset.colNames = newColumnOrder.map(t => t._3.id + "." + t._1)
    joinDataset.colNameSet = joinDataset.colNames.toSet
    joinDataset.erroneous = erroneous || other.erroneous
    joinDataset.containsArrays = containsArrays || other.containsArrays
    joinDataset.containedNestedObjects = containedNestedObjects | other.containedNestedObjects
    val byKey = other.rows.groupBy(c => OldLoadedRelationalDataset.getCellValueAsString(c(otherJoinColIndex)))
    rows.foreach(r => {
      val valueInJoinColumn = OldLoadedRelationalDataset.getCellValueAsString(r(myJoinColIndex))
      byKey.getOrElse(valueInJoinColumn,Seq())
        .foreach(matchingRow => {
          joinDataset.rows += constructRow(r,this,matchingRow,other,newColumnOrder)
        })
    })
    joinDataset
  }


  def isSorted(colNames: IndexedSeq[String]): Boolean = {
    var sorted = true
    var it = colNames.iterator
    var prev = it.next()
    while(it.hasNext && sorted){
      val cur = it.next()
      if(cur < prev)
        sorted = false
      prev = cur
    }
    sorted
  }

  /***
   * Checks tuple set equality (assuming same order of columns in both datasets)
   *
   * @param other
   * @return
   */
  def tupleSetEqual(other: OldLoadedRelationalDataset): Boolean = {
    rows.toSet == other.rows.toSet
  }

  def getProjection(newID:String, columnRenames: IndexedSeq[(String,String)]) = {
    val (myColNames,projectedColnames) = columnRenames.unzip
    assert(myColNames.toSet.subsetOf(colNameSet))
    val projected = new OldLoadedRelationalDataset(newID,version)
    projected.colNames = projectedColnames
    projected.colNameSet = projectedColnames.toSet
    projected.containedNestedObjects = containedNestedObjects
    projected.containsArrays = containsArrays
    projected.erroneous = erroneous
    val columnIndexMap = colNames.zipWithIndex.toMap
    for(i <- 0 until rows.size) {
      val newRow = ArrayBuffer[JsonElement]()
      for (myName <- myColNames) {
        val j = columnIndexMap(myName)
        newRow += rows(i)(j)
      }
      projected.rows +=newRow
    }
    projected
  }


  def getColumnObject(j: Int) = {
    val values:ArrayBuffer[String] = getColContentAsString(j)
    Column(id,version.format(IOService.dateTimeFormatter),colNames(j),values)
  }

  def exportToCSV(file:File) = {
    val pr = new PrintWriter(file)
    pr.println(toCSVLineString(colNames))
    rows.foreach( r => {
      pr.println(toCSVLineString(r.map(OldLoadedRelationalDataset.getCellValueAsString(_))))
    })
    pr.close()
  }

  private def toCSVLineString(line:collection.Seq[String]) = {
    line.map("\"" + _ + "\"").mkString(",")
  }

  def getColContentAsString(j: Int) = {
    val values = scala.collection.mutable.ArrayBuffer[String]()
    for (i <- 0 until rows.size) {
      val jsonValue = rows(i)(j)
      values+= OldLoadedRelationalDataset.getCellValueAsString(jsonValue)
    }
    values
  }

  def exportColumns(writer:PrintWriter, skipNumeric:Boolean = true) = {
    if(!isEmpty && !erroneous) {
      for (i <- 0 until rows(0).size) {
        val col = getColumnObject(i)
        if(!skipNumeric || !col.isNumeric) {
          val json = col.toLSHEnsembleDomain.toJson()
          writer.println(json)
        }
      }
    }
  }

  def getAsTableString(rows: Set[Set[(String, JsonElement)]]) = {
    val schema = rows.head.map(_._1).toIndexedSeq.sorted
    val content = rows.map(_.toIndexedSeq.sortBy(_._1).map(_._2)).toIndexedSeq
    TableFormatter.format(Seq(schema) ++ content)
  }

  def print() = {
    logger.debug("\n" + TableFormatter.format(Seq(colNames) ++ rows))
  }

  private def getTupleMultiSet = {
    rows.map(r => {
      assert(r.size == colNames.size)
      colNames.zip(r).toSet
    }).groupBy(identity)
  }

  val nestedLevelSeparator = "_"

  var colNames = IndexedSeq[String]()
  private var colNameSet = Set[String]()
  private var containedNestedObjects = false
  var containsArrays = false
  var erroneous = false

  def setSchema(firstObj:JsonObject) = {
    colNames = extractNestedKeyValuePairs(firstObj)
        .map(_._1)
    colNameSet = colNames.toSet
  }

  def setSchema(array:JsonArray) = {
    val it = array.iterator()
    val finalSchema = mutable.HashSet[String]()
    while (it.hasNext) {
      val curObj = it.next().getAsJsonObject
      val schema = extractNestedKeyValuePairs(curObj)
      finalSchema ++= schema.map(_._1)
    }
    colNames = finalSchema.toIndexedSeq.sorted
    colNameSet = finalSchema.toSet
  }

  private def extractNestedKeyValuePairs(firstObj: JsonObject): IndexedSeq[(String,JsonElement)] = extractNestedKeyValuePairs("",firstObj)

  private def extractNestedKeyValuePairs(prefix:String, firstObj: JsonObject): IndexedSeq[(String,JsonElement)] = {
    val keyPrefix = if(prefix.isEmpty) "" else prefix + nestedLevelSeparator
    firstObj.keySet().asScala.flatMap(k => {
      firstObj.get(k) match {
        case e:JsonArray => {
          containsArrays = true
          IndexedSeq((keyPrefix + k,e))
        }
        case e:JsonPrimitive =>
          IndexedSeq((keyPrefix + k,e))
        case e:JsonObject => {
          containedNestedObjects = true
          extractNestedKeyValuePairs(keyPrefix + nestedLevelSeparator + k, firstObj.get(k).getAsJsonObject)
        }
        case _ => {
          erroneous = true
          throw new AssertionError("unmatched case")
        }
      }
    }).toIndexedSeq.sortBy(_._1)
  }

  def isEmpty = colNames.isEmpty || rows.isEmpty

  def appendRow(obj: JsonObject,strictSchemaCompliance:Boolean = false) = {
    val kvPairs = extractNestedKeyValuePairs(obj)
      .toMap
    if(!kvPairs.keySet.subsetOf(colNameSet) || (strictSchemaCompliance && kvPairs.keySet!=colNameSet)) {
      erroneous=true
      throw new SchemaMismatchException
    }
    val row = colNames.map(k => kvPairs.getOrElse(k,JsonNull.INSTANCE))
    rows += row
  }

  def ncols = colNames.size

  def getNumberFromCell(value: JsonPrimitive): Any = {
    val num = value.getAsString
    val isInteger = num.matches("[-+]?[0-9]+")
    if (isInteger)
      BigInt(value.getAsBigInteger)
    else{
      value.getAsDouble
    }
  }

  def toScalaArray(getAsJsonArray: JsonArray) = {
    val result = mutable.ArrayBuffer[Any]()
    for(i <- 0 until getAsJsonArray.size()){
      result += getCellValue(getAsJsonArray.get(i))
    }
    result
  }

  def getCellValue(c: JsonElement): Any = {
    if(c.isJsonNull) null
    else if(c.isJsonArray) toScalaArray(c.getAsJsonArray)
    else if(c.isJsonPrimitive && c.getAsJsonPrimitive.isBoolean) c.getAsBoolean
    else if(c.isJsonPrimitive && c.getAsJsonPrimitive.isNumber)
      getNumberFromCell(c.getAsJsonPrimitive)
    else if(c.isJsonPrimitive && c.getAsJsonPrimitive.isString) c.getAsString
    else
      throw new AssertionError("matching incomplete")
  }

  def toImproved = {
    val improvedDataset = simplified.RelationalDataset(id,
      version,
      colNames.map(cn => simplified.Attribute(cn,-1)),
      rows.map(r => simplified.RelationalDatasetRow(-1,r.map(c => getCellValue(c)))))
    improvedDataset
  }

}
object OldLoadedRelationalDataset {
  val NULL_VALUE = ""

  def getCellValueAsString(jsonValue:JsonElement):String = {
    jsonValue match {
      case primitive: JsonPrimitive => primitive.getAsString
      case array: JsonArray =>array.toString
      case _: JsonNull => OldLoadedRelationalDataset.NULL_VALUE
      case _ => throw new AssertionError("Switch case finds unhalndeld option")
    }
  }
}
