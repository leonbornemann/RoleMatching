package de.hpi.dataset_versioning.db_synthesis.bottom_up

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.Change

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class FieldLineage(val field:Field,val lineage:mutable.TreeMap[LocalDate,Any]){

  def valueSet = {
    lineage.values.toSet
  }

}

object FieldLineage {

  def partitionToEquivalenceClasses(group: IndexedSeq[FieldLineage]) = {
    //TODO: account for None-values!
    group.groupBy(_.lineage)
      .map(_._2)
  }

  def fromCangeList(field: Field, changeList: ArrayBuffer[Change]) = {
    if(changeList.exists(_.newValue=="PRIUS") && field.tableID=="tfm3-3j95" && field.entityID==1010)
      println()
    val lineage = mutable.TreeMap[LocalDate,Any]()
    changeList.foreach(c => lineage.addOne((c.t,c.newValue)))
    new FieldLineage(field,lineage)
  }

}
