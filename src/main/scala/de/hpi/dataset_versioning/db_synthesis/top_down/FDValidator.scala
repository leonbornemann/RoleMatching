package de.hpi.dataset_versioning.db_synthesis.top_down

import java.io.File
import java.time.LocalDate
import java.util

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}
import de.metanome.algorithms.normalize.Main

import scala.jdk.CollectionConverters._
import scala.collection.mutable

class FDValidator(subdomain:String,id:String) extends StrictLogging{

  val temporalSchema = TemporalSchema.load(id)

  var prefixTree = new PrefixTree()

  def readFDs(id:String,date:LocalDate) :collection.Map[java.util.BitSet, java.util.BitSet] = {
    val fdFile = DBSynthesis_IOService.getFDFile(subdomain,id,date)
    val csvFile = DBSynthesis_IOService.getExportedCSVFile(subdomain,id,date)
    de.metanome.algorithms.normalize.Main.getFdsForFile(id,csvFile.toPath,fdFile.toPath).asScala
  }

  def translateFDPart(left: util.BitSet, posToID: Map[Int, Int]) = {
    var i = left.nextSetBit(0)
    val colIDs = mutable.ArrayBuffer[Int]()
    while (i >= 0) {
      if(!posToID.contains(i))
        println()
      colIDs += posToID(i)
      // operate on index i here
      i = left.nextSetBit(i + 1)
    }
    colIDs.sorted
  }

  def translateFDs(fds: collection.Map[util.BitSet, util.BitSet], date: LocalDate):collection.Map[collection.IndexedSeq[Int],collection.IndexedSeq[Int]] = {
    val posToID = temporalSchema.attributes
      .withFilter(al => !al.valueAt(date)._2.isNE)
      .map(al => {
        val attr = al.valueAt(date)._2.attr.get
        (attr.position.get,attr.id)
      }).toMap
    fds.map{case (left,right) => {
      (translateFDPart(left,posToID),translateFDPart(right,posToID))
    }}
  }

  def reverseTranslateFDPart(left: collection.IndexedSeq[Int], idToPos: Map[Int, Int]) = {
    val bs = new util.BitSet()
    var isComplete = true
    left.foreach(id => {
      if(!idToPos.contains(id)) {
        isComplete = false
        logger.warn(s"Skipping column $id, because it is not present in the final version")
      } else
        bs.set(idToPos(id))
    }) //TODO: there could be a non-match here what do we do then?
    (bs,isComplete)
  }

  def reverseTranslateFDs(fds: Iterator[(collection.IndexedSeq[Int], collection.IndexedSeq[Int])], date: LocalDate): util.Map[util.BitSet, util.BitSet] = {
    val idToPos = temporalSchema.attributes
      .withFilter(al => !al.valueAt(date)._2.isNE)
      .map(al => {
      if(!al.valueAt(date)._2.attr.isDefined){
        println()
      }
      val attr = al.valueAt(date)._2.attr.get
      (attr.id,attr.position.get)
    }).toMap
    val translated = fds.toSeq.map{case (left,right) => {
      (reverseTranslateFDPart(left,idToPos),reverseTranslateFDPart(right,idToPos))
    }}
    val filtered = translated
      .filter(t => t._1._2 && t._2._1.nextSetBit(0) != -1) //filter out all fds that are not completely there on the LHS and have at least one column on the RHS
      .map(t => (t._1._1,t._2._1))
    if(filtered.size!=translated.size)
      logger.warn(s"Filtered out ${translated.size - filtered.size} fds because at least one column on the LHS was not present or RHS was empty")
    //TODO: it is not that simple - how do we deal with inserted/deleted columns when last table is normalized?
    filtered.toMap.asJava
  }

  def getFDIntersection: java.util.Map[java.util.BitSet, java.util.BitSet] = {
    val attributeLineagesByID = temporalSchema.byID
    val files = DBSynthesis_IOService.getSortedFDFiles(subdomain,id)
    //initialize fds:
    val f = files(0)
    var curDate = LocalDate.parse(f.getName.split("\\.")(0),IOService.dateTimeFormatter)
    val firstFDs = readFDs(id,curDate)
    val fdsWithCOLIDS = translateFDs(firstFDs,curDate)
    prefixTree.initializeFDSet(fdsWithCOLIDS)
    for(i <- 1 until files.size){
      val f = files(i)
      curDate = LocalDate.parse(f.getName.split("\\.")(0),IOService.dateTimeFormatter)
      val newFDs = readFDs(id,curDate)
      val fdsWithCOLIDS = translateFDs(newFDs,curDate)
      val intersectedFDs = prefixTree.intersectFDs(fdsWithCOLIDS)
          .filter(fd => fd._1.forall(colID => attributeLineagesByID(colID).valueAt(curDate)._2.exists)) //filters out fds that have an element in LHS that does not exist at curDate
      prefixTree = new PrefixTree
      prefixTree.initializeFDSet(intersectedFDs)
    }
    //translate back:
    val result = reverseTranslateFDs(prefixTree.root.iterator,curDate)
    logger.debug(s"found ${result.size()} fds in the intersection over all timestamps")
    result
  }
}
