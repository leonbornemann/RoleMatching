package de.hpi.dataset_versioning.db_synthesis.bottom_up

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.Change
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class FieldLineage(val field:Field,val lineage:ValueLineage){

  def valueSet = {
    lineage.lineage.values.toSet
  }

}

object FieldLineage {

  def partitionToEquivalenceClasses(group: IndexedSeq[FieldLineage]) = {
    //TODO: account for None-values!
    group.groupBy(_.lineage)
      .map(_._2)
  }

  def fromCangeList(field: Field, changeList: ArrayBuffer[Change]) = {
    val lineage = mutable.TreeMap[LocalDate,Any]()
    changeList.foreach(c => lineage.addOne((c.t,c.value)))
    assert(changeList.exists(_.t==IOService.STANDARD_TIME_FRAME_START))
    new FieldLineage(field,ValueLineage(lineage))
  }

}
