package de.hpi.socrata.change

import de.hpi.socrata.DatasetInstance
import de.hpi.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.socrata.io.Socrata_IOService
import de.hpi.socrata.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.socrata.simplified.{Attribute, RelationalDataset}
import de.hpi.socrata.tfmp_input.table.nonSketch.FactLineage
import de.hpi.role_matching.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate
import scala.collection.mutable

class IDSpecificChangeExporter(id: String, var allVersions: IndexedSeq[LocalDate]) {

  val fieldToLineageMap = mutable.HashMap[(Long,Int),FactLineage]()
  val datasetExistence = mutable.TreeMap[TimeInterval,Boolean]()
  val columnExistence = mutable.TreeMap[Int,mutable.TreeMap[TimeInterval,Boolean]]()
  val colIDTOAttributeMap:mutable.HashMap[Int,mutable.HashMap[LocalDate,Attribute]]=mutable.HashMap()

  //returns the first date where we find the column to be defined
  def createNewLineageWithAbsentValues(e: Long, p: Int,curVersion:LocalDate) = {
    val field = (e,p)
    fieldToLineageMap.put(field,FactLineage())
    if(columnExistence(p).last._1.`end` != datasetExistence.last._1.`end`){
      println()
    }
    assert(columnExistence(p).last._1.`end` == datasetExistence.last._1.`end`)
    val columnExistenceIterator = columnExistence(p).iterator
    val datasetExistenceIterator = datasetExistence.iterator
    var curColumnExistence = columnExistenceIterator.nextOption()
    var curDatasetExistence = datasetExistenceIterator.nextOption()
    assert(curColumnExistence.get._1.begin == curDatasetExistence.get._1.begin)
    while(curColumnExistence.isDefined && curDatasetExistence.get._1.begin.isBefore(curVersion)){
      assert(curDatasetExistence.isDefined)
      val beginCol = curColumnExistence.get._1.begin
      val beginDS = curDatasetExistence.get._1.begin
      val endCol = curColumnExistence.get._1.`end`.get
      val endDS = curDatasetExistence.get._1.`end`.get
      assert(beginCol == beginDS)
      if(endCol == endDS){
        //update for timeinterval:
        val value = getValueForMissingField(curColumnExistence, curDatasetExistence)
        addToLineageIfNotEqualToPrevious(beginDS: LocalDate, field, value)
        curColumnExistence = columnExistenceIterator.nextOption()
        curDatasetExistence = datasetExistenceIterator.nextOption()
      } else if(endCol.isBefore(endDS)) {
        val value = getValueForMissingField(curColumnExistence,curDatasetExistence)
        addToLineageIfNotEqualToPrevious(beginDS,field,value)
        curDatasetExistence = Some(TimeInterval(curColumnExistence.get._1.endOrMax.plusDays(1),curDatasetExistence.get._1.`end`),curDatasetExistence.get._2)
        curColumnExistence = columnExistenceIterator.nextOption()
      } else {
        assert(endDS.isBefore(endCol))
        val value = getValueForMissingField(curColumnExistence,curDatasetExistence)
        addToLineageIfNotEqualToPrevious(beginDS,field,value)
        curColumnExistence = Some(TimeInterval(curDatasetExistence.get._1.endOrMax.plusDays(1),curColumnExistence.get._1.`end`),curColumnExistence.get._2)
        curDatasetExistence = datasetExistenceIterator.nextOption()
      }
      if(curColumnExistence.isEmpty ^ curDatasetExistence.isEmpty){
        println()
      }
    }
  }

  private def getValueForMissingField(curColumnExistence: Option[(TimeInterval, Boolean)], curDatasetExistence: Option[(TimeInterval, Boolean)]) = {
    if (curDatasetExistence.get._2) {
      if(curColumnExistence.get._2){
        ReservedChangeValues.NOT_EXISTANT_ROW
      } else{
        ReservedChangeValues.NOT_EXISTANT_COL
      }
    } else {
      ReservedChangeValues.NOT_EXISTANT_DATASET
    }
  }

  def addDatasetDelete(curTimeInterval: TimeInterval) = {
    datasetExistence.put(curTimeInterval,false)
    columnExistence.values.foreach(_.put(curTimeInterval,false))
    //add this to tracked fields:
    fieldToLineageMap.keySet.foreach(field => {
      addToLineageIfNotEqualToPrevious(curTimeInterval.begin,field,ReservedChangeValues.NOT_EXISTANT_DATASET)
    })
  }

  def processNewVersion(curTimeInterval: TimeInterval) = {
    //update ds existence
    datasetExistence.put(curTimeInterval,true)
    val curVersion = curTimeInterval.begin
    val ds = RelationalDataset.load(id,curVersion)
    updateAttributeExistence(curTimeInterval, ds)
    updateFieldLineages(curVersion, ds)
  }

  private def updateFieldLineages(curVersion: LocalDate, ds: RelationalDataset) = {
    val seenFieldLineages = mutable.HashSet[(Long, Int)]()
    ds.rows.foreach(r => {
      val e = r.id
      //TODO: also update all maintained lineages that we did not cover
      (0 until r.fields.size).foreach(attrIndex => {
        val p = ds.attributes(attrIndex).id
        val field = (e, p)
        seenFieldLineages.add(field)
        val value = r.fields(attrIndex)
        if (fieldToLineageMap.contains(field)) {
          addToLineageIfNotEqualToPrevious(curVersion, field, value)
        } else {
          //init lineage based on column and dataset info:
          createNewLineageWithAbsentValues(e, p,curVersion)
          assert(curVersion == GLOBAL_CONFIG.STANDARD_TIME_FRAME_START && fieldToLineageMap(field).lineage.isEmpty || fieldToLineageMap(field).lineage.lastKey.isBefore(curVersion))
          fieldToLineageMap(field).lineage.put(curVersion, value)
        }
      })
    })
    //handle deleted rows:
    val attrSetThisVersion = ds.attributes.map(_.id).toSet
    fieldToLineageMap.keySet.diff(seenFieldLineages).foreach { case (e, p) => {
      val field = (e, p)
      if (attrSetThisVersion.contains(p)) {
        addToLineageIfNotEqualToPrevious(curVersion, field, ReservedChangeValues.NOT_EXISTANT_ROW)
      } else {
        addToLineageIfNotEqualToPrevious(curVersion, field, ReservedChangeValues.NOT_EXISTANT_COL)
      }
    }
    }
  }

  private def addToLineageIfNotEqualToPrevious(curVersion: LocalDate, field: (Long, Int), value: Any) = {
    val curLineage = fieldToLineageMap(field)
    val last = curLineage.lineage.lastOption
    if(! (last.isEmpty || last.get._1.isBefore(curVersion)))
      println()
    assert(last.isEmpty || last.get._1.isBefore(curVersion))
    if (last.isEmpty || last.get._2 != value)
      curLineage.lineage.put(curVersion, value)
  }

  private def updateAttributeExistence(curTimeInterval: TimeInterval, ds: RelationalDataset) = {
    ds.attributes.foreach(a => colIDTOAttributeMap
      .getOrElseUpdate(a.id,mutable.HashMap[LocalDate,Attribute]())
      .put(curTimeInterval.begin,a)
    )
    val attrSetThisVersion = ds.attributes.map(_.id).toSet
    val firstTimeAttrInserts = attrSetThisVersion.diff(columnExistence.keySet)
    val attrsAbsentInThisVersion = columnExistence.keySet.diff(attrSetThisVersion)
    val knownAttrsInThisVersion = columnExistence.keySet.intersect(attrSetThisVersion)
    attrsAbsentInThisVersion.foreach(id => columnExistence(id).put(curTimeInterval, false))
    firstTimeAttrInserts.foreach(id => {
      assert(!columnExistence.contains(id))
      val newExistenceMap = mutable.TreeMap[TimeInterval, Boolean]()
      if(curTimeInterval.begin!=GLOBAL_CONFIG.STANDARD_TIME_FRAME_START){
        val prevInterval = TimeInterval(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, Some(curTimeInterval.begin.minusDays(1)))
        newExistenceMap.put(prevInterval, false)
      }
      newExistenceMap.put(curTimeInterval, true)
      columnExistence.put(id, newExistenceMap)
    })
    knownAttrsInThisVersion.foreach(id => columnExistence(id).put(curTimeInterval, true))
    columnExistence.foreach{case (id,map) => {
      val keys = map.keys.toIndexedSeq
      (1 until keys.size).map(i => {
        assert(keys(i-1).intersect(keys(i)).isEmpty)
      })
    }}
  }

  def exportAllChanges() = {
    val firstVersion = allVersions(0)
    if(firstVersion.isAfter(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START)){
      allVersions = IndexedSeq(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START) ++ allVersions
    }
    for( i<- 0 until allVersions.size){
      val curVersion = allVersions(i)
      val endOfInterval = if(i==allVersions.size-1) None else Some(allVersions(i+1).minusDays(1))
      val curTimeInterval = TimeInterval(curVersion,endOfInterval)
      if(versionExists(curVersion)){
        //process insert or update
        processNewVersion(curTimeInterval)
      } else {
        //process dataset delete
        addDatasetDelete(curTimeInterval)
      }
    }
    runtIntegrityCheck
    val finalChangeCube = ChangeCube(id,colIDTOAttributeMap)
    fieldToLineageMap.flatMap{case ((e,p),vl) => {
      vl.lineage.map{case (t,v) => finalChangeCube.addChange(Change(t,e,p,v))}
    }}
    finalChangeCube.toJsonFile(new File(Socrata_IOService.getChangeFile(id)))
    TemporalSchema.fromTemporalTable(finalChangeCube.toTemporalTable()).writeToStandardFile()
  }

  private def runtIntegrityCheck = {
    fieldToLineageMap.foreach { case ((e, p), fieldLineage) => {
      val valuesInOrder = fieldLineage.lineage.toIndexedSeq.map(_._2)
      assert(fieldLineage.lineage.firstKey == GLOBAL_CONFIG.STANDARD_TIME_FRAME_START)
      (0 until valuesInOrder.size - 1).foreach(i => {
        assert(valuesInOrder(i) != valuesInOrder(i + 1))
      })
      //check for correct dataset deletes:
      var prevWasPresent = datasetExistence.head._2
      datasetExistence.tail.foreach { case (ti, wasPresent) => {
        if (!wasPresent && prevWasPresent) {
          assert(fieldLineage.lineage(ti.begin) == ReservedChangeValues.NOT_EXISTANT_DATASET)
        }
        prevWasPresent = wasPresent
      }}
      //TODO: check for correct column deletes:
    }
    }
  }

  private def versionExists(curVersion: LocalDate) = {
    new File(Socrata_IOService.getSimplifiedDatasetFile(DatasetInstance(id, curVersion))).exists()
  }
}
