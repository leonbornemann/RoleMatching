package de.hpi.tfm.data.socrata.simplified

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata
import de.hpi.tfm.data.socrata.DatasetInstance
import de.hpi.tfm.data.socrata.history.DatasetVersionHistory
import de.hpi.tfm.io.IOService
import org.apache.commons.csv.{CSVFormat, CSVParser}

import java.io.{File, FileReader}
import java.time.LocalDate
import scala.collection.mutable

class ColumnOrderRestorer extends StrictLogging{


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

  def restoreAllInVersion(version:LocalDate) = {
    IOService.cacheMetadata(version)
    val dir = IOService.getSimplifiedDataDir(version)
    var count =0
    dir.listFiles.foreach(f => {
      count += 1
      restoreInDataset(IOService.filenameToID(f),version)
      if (count % 1000 == 0) {
        val total = Seq(attrPositionNotFound, attrTailFound, attrWithoutLeadingUnderscoreFound, attrExactPositionFound).sum.toDouble
        logger.debug(s"${attrExactPositionFound / total},${attrWithoutLeadingUnderscoreFound / total},${attrTailFound / total},${attrPositionNotFound / total}")
      }
    })
  }

  def restoreForAllForIdList(ids:Set[String]) = {
    val histories = DatasetVersionHistory.load()
    val versionToIds = histories
      .filter(h => ids.contains(h.id))
      .flatMap(h => h.versionsWithChanges.map(v => (h.id,v)))
      .groupMap(_._2)(_._1)
    versionToIds.foreach{case (v,ids) => {
      ids.foreach( id => restoreInDataset(id,v))
      IOService.cachedMetadata.clear()
    }}
  }

  def getBaseNameToPosition(id: String): collection.Map[String, Int] = {
    val csvFile = IOService.getCSVFile(id)
    if(!csvFile.exists())
      Map()
    else {
      val reader = new FileReader(csvFile)
      val parser = CSVParser.parse(reader, CSVFormat.DEFAULT.withQuote('\"'))
      val headers = parser.iterator().next()
      val headerToPosition = (0 until headers.size()).map( i=> (headers.get(i),i))
          .toMap
        //.map{case (k,v) => (k,v.intValue())}
      reader.close()
      headerToPosition
    }
  }

  def correctInDataset(ds:RelationalDataset) = {
    val dsOld = IOService.tryLoadDataset(DatasetInstance(ds.id,ds.version),true)
    assert(dsOld.colNames.size == ds.attributes.size)
    ds.attributes.zip(dsOld.colNames)
      .foreach{case (a,newName) => {
        a.name = newName
        a.humanReadableName = None
        a.id = -1
      }}
    //just overwrite the names I guess?
    //we need to redo only the column matching!
  }

  def restoreInDataset(id:String, version:LocalDate,runCorrection:Boolean = false) = {
    IOService.cacheMetadata(version)
    val mdForDs = IOService.cachedMetadata(version).get(id)
    var ds: RelationalDataset = null
    try {
      ds = IOService.loadSimplifiedRelationalDataset(socrata.DatasetInstance(id, version))
    } catch {
      case e: Throwable => {
        logger.debug(s"Error while loading $id, (version $version)")
      }
    }
    if(ds!=null) {
      if (runCorrection) {
        correctInDataset(ds)
      }
      val baseNameToPosition: collection.Map[String, Int] = getBaseNameToPosition(id)
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
      //store the attributes in new order --> we need to do this for all values as well!
      ds.sortColumnsByAttributePosition()
      //also store the humanreadable name if we matched it:
      if (mdForDs.isDefined) {
        matchedAttributes.foreach(a => {
          val mdColnameToID = mdForDs.get.resource.columns_field_name.zipWithIndex
            .toMap
          if (mdColnameToID.contains(a.name))
            a.humanReadableName = Some(mdForDs.get.resource.columns_name(mdColnameToID(a.name)))
        })
      }
      ds.toJsonFile(new File(IOService.getSimplifiedDatasetFile(socrata.DatasetInstance(id, version)) + "_repaired.json?"))
    }
  }

}
