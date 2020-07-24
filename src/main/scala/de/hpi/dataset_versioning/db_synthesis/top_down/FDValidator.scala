package de.hpi.dataset_versioning.db_synthesis.top_down

import java.io.File
import java.time.LocalDate
import java.util

import de.hpi.dataset_versioning.data.change.TemporalTable
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}
import collection.JavaConverters._

import scala.collection.mutable

class FDValidator(id:String) {

  val temporalTable = TemporalTable.load(id)

  var prefixTree = new PrefixTree()

  def readFDs(f: File) :Map[java.util.BitSet, java.util.BitSet] = ???

  def translateFDPart(left: util.BitSet, posToID: Map[Int, Int]) = {
    var i = left.nextSetBit(0)
    val colIDs = mutable.ArrayBuffer[Int]()
    while (i >= 0) {
      colIDs += posToID(i)
      // operate on index i here
      i = left.nextSetBit(i + 1)
    }
    colIDs.sorted
  }

  def translateFDs(fds: Map[util.BitSet, util.BitSet], date: LocalDate):Map[collection.IndexedSeq[Int],collection.IndexedSeq[Int]] = {
    val posToID = temporalTable.attributes.map(al => {
      val attr = al.valueAt(date)._2.attr.get
      (attr.position.get,attr.id)
    }).toMap
    fds.map{case (left,right) => {
      (translateFDPart(left,posToID),translateFDPart(right,posToID))
    }}
  }

  def reverseTranslateFDPart(left: collection.IndexedSeq[Int], idToPos: Map[Int, Int]) = {
    val bs = new util.BitSet()
    left.foreach(id => bs.set(idToPos(id)))
    bs
  }

  def reverseTranslateFDs(fds: Iterator[(collection.IndexedSeq[Int], collection.IndexedSeq[Int])], date: LocalDate): util.Map[util.BitSet, util.BitSet] = {
    val idToPos = temporalTable.attributes.map(al => {
      val attr = al.valueAt(date)._2.attr.get
      (attr.id,attr.position.get)
    }).toMap
    fds.toSeq.map{case (left,right) => {
      (reverseTranslateFDPart(left,idToPos),reverseTranslateFDPart(right,idToPos))
    }}.toMap.asJava
  }

  def getFDIntersection: java.util.Map[java.util.BitSet, java.util.BitSet] = {
    val files = new File(DBSynthesis_IOService.FDDIR + File.separator + id)
      .listFiles()
      .toIndexedSeq
    //initialize fds:
    val f = files(0)
    val firstFDs = readFDs(f)
    val date = LocalDate.parse(f.getName.split("\\.")(0),IOService.dateTimeFormatter)
    //TODO: translate FD bitsets to column ids:
    val fdsWithCOLIDS = translateFDs(firstFDs,date)
    prefixTree.initializeFDSet(fdsWithCOLIDS)
    for(i <- 1 until files.size){
      val f = files(i)
      val newFDs = readFDs(f)
      val date = LocalDate.parse(f.getName.split("\\.")(0),IOService.dateTimeFormatter)
      val fdsWithCOLIDS = translateFDs(newFDs,date)
      val intersectedFDs = prefixTree.intersectFDs(fdsWithCOLIDS)
      prefixTree = new PrefixTree
      prefixTree.initializeFDSet(intersectedFDs)
    }
    //translate back:
    reverseTranslateFDs(prefixTree.root.iterator,date)
  }
}
