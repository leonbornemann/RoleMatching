package de.hpi.dataset_versioning.data.change.temporal_tables

import java.time.LocalDate

import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence

import scala.collection.mutable

@SerialVersionUID(3L)
class AttributeLineage(val attrId:Int,val lineage:mutable.TreeMap[LocalDate,AttributeState]) extends Serializable{

  def lastName = lineage.filter(_._2.exists).last._2.attr.get.name

  def createCopyToNewId(newID: Int) = {
    val lineageMapped = lineage.map { case (t, as) => {
      if (as.attr.isDefined) {
        val attr = Attribute(as.attr.get.name, newID, as.attr.get.position, as.attr.get.humanReadableName)
        (t, AttributeState(Some(attr)))
      } else {
        (t, as)
      }
    }}
    new AttributeLineage(newID,lineageMapped)
  }

  def unionDisjoint(b: AttributeLineage, newID:Int) = {
    val myIdMapped = this.createCopyToNewId(newID)
    val otherIdMapped = b.createCopyToNewId(newID)
    //assert disjointedness:
    assert(myIdMapped.activeTimeIntervals.intersect(otherIdMapped.activeTimeIntervals).isEmpty)
    new AttributeLineage(attrId,myIdMapped.lineage ++ otherIdMapped.lineage)
  }

  def nameSet = lineage.withFilter(_._2.exists).map(_._2.attr.get.name).toSet

  def lastDefinedValue = {
    val lastElem = lineage.last
    if(lastElem._2.exists){
      lastElem._2.attr.get
    } else{
      val a = lineage.maxBefore(lastElem._1)
      assert(a.isDefined && a.get._2.exists)
      a.get._2.attr.get
    }
  }


  def activeTimeIntervals = {
    var curBegin = lineage.head._1
    var curEnd:LocalDate = null
    var activeTimeIntervals = mutable.ArrayBuffer[TimeInterval]()
    for((ts,value) <- lineage){
      if(curEnd == null && value.isNE){
        assert(curBegin!=null)
        activeTimeIntervals.append(TimeInterval(curBegin,Some(ts.minusDays(1))))
        curBegin=null
        curEnd=null
      } else if(curBegin==null){
        assert(!value.isNE)
        curBegin = ts
      }
    }
    //add last time period
    if(curBegin!=null)
      activeTimeIntervals += TimeInterval(curBegin,None)
    activeTimeIntervals
    new TimeIntervalSequence(activeTimeIntervals.toIndexedSeq)
  }

  def valueAt(timestamp: LocalDate) = {
    if(lineage.contains(timestamp))
      (timestamp,lineage(timestamp))
    else {
      val atTs = lineage.maxBefore(timestamp)
      if(atTs.isDefined)
        atTs.get
      else
        (timestamp,AttributeState.NON_EXISTANT)
    }
  }


  override def toString: String = "[" + lineage.values.toSeq.map(_.displayName).mkString("|") + "]"
}