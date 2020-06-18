package de.hpi.dataset_versioning.data.simplified

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object ColumnOrderRestore extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val version = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val dir = IOService.getSimplifiedDataDir(version)
  val id = if(args.length==2) Some(args(2)) else None
  IOService.cacheMetadata(version)
  var attrExactPositionFound:Long = 0
  var attrWithoutLeadingUnderscoreFound:Long = 0
  var attrTailFound:Long = 0
  var attrPositionNotFound:Long = 0

  def setPosition(a: Attribute,pos:Int, usedPositions: mutable.HashSet[Int]) = {
    if(!usedPositions.contains(pos)) {
      a.position = Some(pos)
      usedPositions += pos
      true
    } else {
      false
    }
  }

  if(id.isDefined){
    val f = IOService.getSimplifiedDatasetFile(DatasetInstance(id.get,version))
    logger.debug(s"Redoing Column Ordering for single File: $f")
    restoreColumnOrderForFile(new File(f))
  } else {
    logger.debug(s"Redoing Column Ordering for all datasets in version  $version")
    var count = 0
    dir.listFiles.foreach(f => {
      count += 1
      restoreColumnOrderForFile(f)
      if (count % 1000 == 0) {
        val total = Seq(attrPositionNotFound, attrTailFound, attrWithoutLeadingUnderscoreFound, attrExactPositionFound).sum.toDouble
        logger.debug(s"${attrExactPositionFound / total},${attrWithoutLeadingUnderscoreFound / total},${attrTailFound / total},${attrPositionNotFound / total}")
      }
    })
  }

  def restoreColumnOrderForFile(f:File) = {
    val id = IOService.filenameToID(f)
    var ds: RelationalDataset = null
    try {
      ds = IOService.loadSimplifiedRelationalDataset(DatasetInstance(id, version))
    } catch {
      case e: Throwable => {
        println(id)
        throw e
      }
    }
    val mdForDS = IOService.cachedMetadata(version)(id)
    val baseNameToPosition = mdForDS.resource.columnNameToPosition
    val usedPositions = mutable.HashSet[Int]()
    val matchedAttributes = mutable.HashSet[Attribute]()
    ds.attributes.foreach(a => {
      if (baseNameToPosition.contains(a.name)) {
        val pos = baseNameToPosition(a.name)
        val wasSet = setPosition(a, pos, usedPositions)
        if (wasSet) matchedAttributes.add(a)
        attrExactPositionFound += 1
      }
    })
    ds.attributes.diff(matchedAttributes.toSeq).foreach(a => {
      val key = a.name.substring(1)
      if (a.name.startsWith("_") && baseNameToPosition.contains(key)) {
        val pos = baseNameToPosition(key)
        val wasSet = setPosition(a, pos, usedPositions)
        attrWithoutLeadingUnderscoreFound += 1
        if (wasSet) matchedAttributes.add(a)
      }
    })
    ds.attributes.diff(matchedAttributes.toSeq).foreach(a => {
      val tokens = a.name.split("_")
      if (tokens.size > 0) {
        val key = tokens(tokens.size - 1)
        if (a.name.startsWith("_") && baseNameToPosition.contains(key)) {
          val pos = baseNameToPosition(key)
          val wasSet = setPosition(a, pos, usedPositions)
          attrTailFound += 1
          if (wasSet) matchedAttributes.add(a)
        }
      }
    })
    //also store the humanreadable name if we matched it:
    matchedAttributes.foreach(a => a.humanReadableName = Some(mdForDS.resource.columns_name(a.position.get)))
    var curDefaultPosition = ds.attributes.size
    ds.attributes.diff(matchedAttributes.toSeq).foreach(a => {
      a.position = Some(curDefaultPosition)
      curDefaultPosition += 1
      attrPositionNotFound += 1
    })
    //now the attribute positions are correctly ordered, but not necessarily with correct values
    ds.attributes.sortBy(_.position.get)
      .zipWithIndex
      .foreach { case (a, pos) => a.position = Some(pos) }
    if (ds.attributes.map(_.position.get).sorted.toIndexedSeq != (0 until ds.attributes.size))
      println()
    assert(ds.attributes.map(_.position.get).sorted.toIndexedSeq == (0 until ds.attributes.size))
    //store the attributes in new order
    ds.attributes = ds.attributes.sortBy(_.position.get)
    ds.toJsonFile(new File(IOService.getSimplifiedDatasetFile(DatasetInstance(id, version))))
  }
}
