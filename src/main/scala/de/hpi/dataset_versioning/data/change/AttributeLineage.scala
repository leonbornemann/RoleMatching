package de.hpi.dataset_versioning.data.change

import java.time.LocalDate

import de.hpi.dataset_versioning.data.simplified.Attribute

import scala.collection.mutable

class AttributeLineage(val attrId:Int,val lineage:mutable.TreeMap[LocalDate,AttributeState]) {

  def activeTimeIntervals = {
    var curBegin = lineage.head._1
    var curEnd:LocalDate = null
    var activeTimeIntervals = mutable.ArrayBuffer[TimeInterval]()
    for((ts,value) <- lineage){
      if(curEnd == null && value.isNE){
        assert(curBegin!=null)
        activeTimeIntervals.append(TimeInterval(curBegin,Some(ts)))
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
  }

  def valueAt(timestamp: LocalDate) = lineage.maxBefore(timestamp).get


  override def toString: String = "[" + lineage.values.toSeq.map(_.displayName).mkString("|") + "]"
}