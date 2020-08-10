package de.hpi.dataset_versioning.data.matching

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.{AttributeLineage, TemporalTable}
import de.hpi.dataset_versioning.data.matching.ColumnMatchingRefinementMain.id
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.oneshot.ScriptMain.{allTimestamps, numPossibleConfusions}

import scala.collection.mutable

class ColumnMatchingRefinement(val id:String) {

  var mergesFound = 0

  val allTimestamps = (IOService.STANDARD_TIME_FRAME_START.toEpochDay to IOService.STANDARD_TIME_FRAME_END.toEpochDay)
    .toIndexedSeq
    .map(LocalDate.ofEpochDay(_))


  def sharesNoTimestamp(attrA: AttributeLineage, attrB: AttributeLineage): Boolean = {
    !allTimestamps.exists(ts => attrA.valueAt(ts)._2.exists && attrB.valueAt(ts)._2.exists)
  }

  def sharesNoTimestamp(attrA: AttributeLineage, attrs: collection.Set[AttributeLineage]): Boolean = {
    attrs.forall(attrB => sharesNoTimestamp(attrA, attrB))
  }

  def nameBordersMatch(attrA: AttributeLineage, attrs: collection.Set[AttributeLineage]): Boolean = {
    val sortedByTimestamp = allTimestamps.flatMap(ts => Seq((ts,attrA.valueAt(ts)._2)) ++ attrs.map(al => (ts,al.valueAt(ts)._2)))
      .filter(_._2.exists)
      .sortBy(_._1.toEpochDay)
    if(sortedByTimestamp.map(_._1).toSet.size != sortedByTimestamp.size){
      println()
    }
    assert(sortedByTimestamp.map(_._1).toSet.size == sortedByTimestamp.size)
    var prevAttr = sortedByTimestamp(0)._2.attr.get
    var namesAtBordersMatch = true
    var i = 1
    while (namesAtBordersMatch && i < sortedByTimestamp.size) {
      val curAttr = sortedByTimestamp(i)._2.attr.get
      if (curAttr.id != prevAttr.id) {
        namesAtBordersMatch = curAttr.name == prevAttr.name
      }
      prevAttr = curAttr
      i += 1
    }
    namesAtBordersMatch
  }

  def nameBordersMatch(attrA: AttributeLineage, attrB: AttributeLineage): Boolean = {
    nameBordersMatch(attrA, Set(attrB))
  }

  def getPossibleSaveMerges(attrs: collection.IndexedSeq[AttributeLineage]) = {
    mergesFound = 0
    val mergedLineages = mutable.ArrayBuffer() ++ attrs.map(al => mutable.HashSet[AttributeLineage](al))
    val taken = mutable.HashSet[AttributeLineage]()
    for (i <- 0 until attrs.size) {
      val attrA = attrs(i)
      if (!taken.contains(attrA)) {
        val mergeCandidates = mergedLineages
          .filter(mergedLineage => mergedLineage.forall(_.attrId != attrA.attrId))
          .filter(mergedLineage => sharesNoTimestamp(attrA, mergedLineage) && nameBordersMatch(attrA, mergedLineage))
        if (!mergeCandidates.isEmpty) {
          val toMergeWith = mergeCandidates.head
          toMergeWith.add(attrA)
          mergesFound +=1
        } else {
          //iterate over all others to potentially form a mergedLineage
          for (j <- i + 1 until attrs.size) {
            val attrB = attrs(j)
            if (!taken.contains(attrB) && sharesNoTimestamp(attrA, attrB) && nameBordersMatch(attrA, attrB)) {
              taken += attrB
              mergedLineages.addOne(mutable.HashSet(attrA, attrB))
              mergesFound +=1
            }
          }
        }
      }
    }
    (mergesFound,mergedLineages)
  }

  def refineAndMakeStateConsistent(attrs: mutable.IndexedSeq[AttributeLineage]) = {

    val merges = getPossibleSaveMerges(attrs)
    println()
  }
}
