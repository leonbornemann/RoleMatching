package de.hpi.dataset_versioning.db_synthesis.baseline.index

import de.hpi.dataset_versioning.data.change.temporal_tables.TimeInterval
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.TemporalTableSketch

class ValueLineageIndex[A](val index: Map[IndexedSeq[Map[TimeInterval, A]], IndexedSeq[Int]],val attributeOrderInIndex: IndexedSeq[Int]) {



}
object ValueLineageIndex {

  def buildIndex[A](sketchA: TemporalDatabaseTableTrait[A], indexTimespansByAttributeID: collection.Map[Int, TimeIntervalSequence], attributeOrderInIndex: IndexedSeq[Int]) = {
    val columnsByID = sketchA.columns.map(c => (c.attrID, c)).toMap
    val columnsInIndexOrder = attributeOrderInIndex.map(colID => columnsByID(colID))
    val index = (0 until sketchA.columns.head.fieldLineages.size).groupBy(rowID => {
      val res = columnsInIndexOrder.map(columnSketch => {
        //we need the nonWildCardOverlap of this column
        val timeToExtract = indexTimespansByAttributeID(columnSketch.attrID)
        val cActiveTime = columnSketch.attributeLineage.activeTimeIntervals
        columnSketch.fieldLineages(rowID).valuesAt(timeToExtract.intersect(cActiveTime)) //IMPORTANT: THESE ARE JUST THE HASH VALUES AT THIS TIME, NOT WILDCARDS BEFORE/AFTER
      })
      res
    })
    new ValueLineageIndex(index,attributeOrderInIndex)
  }

}
